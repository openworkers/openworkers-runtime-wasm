//! WebAssembly Worker implementation using Wasmtime Component Model
//!
//! Uses WIT (WebAssembly Interface Types) for type-safe host/guest communication.

use openworkers_core::{
    Event, HttpMethod, HttpRequest, HttpResponse, OperationsHandle, RequestBody, ResponseBody,
    RuntimeLimits, Script, TaskResult, TaskSource, TerminationReason, WorkerCode,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable, bindgen};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

// Generate bindings from the WIT file
bindgen!({
    path: "wit/worker.wit",
    imports: { default: async },
    exports: { default: async },
});

// Re-export the generated types for convenience
use openworkers::worker::types::{
    HttpMethod as WitHttpMethod, HttpRequest as WitHttpRequest, HttpResponse as WitHttpResponse,
};

/// State held by each WASM instance
pub struct WasmState {
    /// WASI context
    wasi: WasiCtx,
    /// Resource table (required by wasmtime component model)
    table: ResourceTable,
    /// Environment variables (for our custom host interface)
    env: HashMap<String, String>,
    /// Abort flag
    #[allow(dead_code)]
    aborted: Arc<AtomicBool>,
    /// Operations handle for fetch, KV, etc. (delegated to runner)
    ops: Option<OperationsHandle>,
}

impl WasmState {
    fn new(
        env: HashMap<String, String>,
        aborted: Arc<AtomicBool>,
        ops: Option<OperationsHandle>,
    ) -> Self {
        // Build WASI context with environment variables
        let mut wasi_builder = WasiCtxBuilder::new();

        for (k, v) in &env {
            wasi_builder.env(k, v);
        }

        Self {
            wasi: wasi_builder.build(),
            table: ResourceTable::new(),
            env,
            aborted,
            ops,
        }
    }
}

// Implement WasiView for WASI support
impl WasiView for WasmState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

// Implement the host interface for WasmState
impl openworkers::worker::host::Host for WasmState {
    async fn log(&mut self, level: u8, message: String) {
        let level_str = match level {
            0 => "DEBUG",
            1 => "INFO",
            2 => "WARN",
            3 => "ERROR",
            _ => "LOG",
        };
        println!("[WASM {}] {}", level_str, message);
    }

    async fn get_env(&mut self, key: String) -> Option<String> {
        self.env.get(&key).cloned()
    }

    async fn fetch(&mut self, request: WitHttpRequest) -> Result<WitHttpResponse, String> {
        let ops = self
            .ops
            .as_ref()
            .ok_or_else(|| "fetch not available: no operations handle".to_string())?;

        // Convert WIT request to core HttpRequest
        let method = match request.method {
            WitHttpMethod::Get => HttpMethod::Get,
            WitHttpMethod::Post => HttpMethod::Post,
            WitHttpMethod::Put => HttpMethod::Put,
            WitHttpMethod::Delete => HttpMethod::Delete,
            WitHttpMethod::Patch => HttpMethod::Patch,
            WitHttpMethod::Head => HttpMethod::Head,
            WitHttpMethod::Options => HttpMethod::Options,
        };

        let body = match request.body {
            None => RequestBody::None,
            Some(bytes) => RequestBody::Bytes(bytes::Bytes::from(bytes)),
        };

        let headers: HashMap<String, String> = request.headers.into_iter().collect();

        let core_request = HttpRequest {
            method,
            url: request.url,
            headers,
            body,
        };

        // Delegate to runner via OperationsHandle
        let response = ops
            .handle_fetch(core_request)
            .await
            .map_err(|e| format!("fetch failed: {}", e))?;

        // Convert core HttpResponse to WIT response
        // Note: Streams are not supported in WASM - they would require async iteration
        let body = match response.body {
            ResponseBody::None => None,
            ResponseBody::Bytes(b) => Some(b.to_vec()),
            ResponseBody::Stream(_) => {
                return Err("streaming responses not supported in WASM".to_string());
            }
        };

        Ok(WitHttpResponse {
            status: response.status,
            headers: response.headers,
            body,
        })
    }
}

// Implement the types interface (required even if it only has types)
impl openworkers::worker::types::Host for WasmState {}

/// WebAssembly Worker using Wasmtime Component Model
pub struct WasmWorker {
    /// Wasmtime engine (can be shared across workers)
    engine: Engine,
    /// Compiled component
    component: Component,
    /// Runtime limits
    limits: RuntimeLimits,
    /// Abort flag
    aborted: Arc<AtomicBool>,
    /// Environment variables from script
    env: HashMap<String, String>,
    /// Operations handle for fetch, KV, etc. (delegated to runner)
    ops: Option<OperationsHandle>,
}

impl WasmWorker {
    /// Create a new WASM worker with an OperationsHandler
    ///
    /// All operations (fetch, log, etc.) go through the runner's OperationsHandler.
    pub async fn new_with_ops(
        script: Script,
        limits: Option<RuntimeLimits>,
        ops: OperationsHandle,
    ) -> Result<Self, TerminationReason> {
        WasmWorker::new(script, limits, Some(ops)).await
    }

    /// Create a new WASM worker from a Component
    pub async fn new(
        script: Script,
        limits: Option<RuntimeLimits>,
        ops: Option<OperationsHandle>,
    ) -> Result<Self, TerminationReason> {
        let limits = limits.unwrap_or_default();

        // Configure engine with async and component model support
        let mut config = Config::new();
        config.async_support(true);

        // Enable fuel-based metering for CPU limiting
        if limits.max_cpu_time_ms > 0 {
            config.consume_fuel(true);
        }

        let engine = Engine::new(&config).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to create engine: {}", e))
        })?;

        // Extract WASM bytes from WorkerCode
        let wasm_bytes = match &script.code {
            WorkerCode::WebAssembly(bytes) => bytes.clone(),
            WorkerCode::JavaScript(_) => {
                return Err(TerminationReason::InitializationError(
                    "WASM runtime cannot execute JavaScript code".to_string(),
                ));
            }
            WorkerCode::Snapshot(_) => {
                return Err(TerminationReason::InitializationError(
                    "WASM runtime cannot execute snapshots".to_string(),
                ));
            }
        };

        // Compile the component
        let component = Component::new(&engine, &wasm_bytes).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to compile component: {}", e))
        })?;

        Ok(Self {
            engine,
            component,
            limits,
            aborted: Arc::new(AtomicBool::new(false)),
            env: script.env.unwrap_or_default(),
            ops,
        })
    }

    /// Execute an event
    pub async fn exec(&mut self, event: Event) -> Result<(), TerminationReason> {
        if self.aborted.load(Ordering::SeqCst) {
            return Err(TerminationReason::Aborted);
        }

        match event {
            Event::Fetch(mut init) => {
                let fetch_init = init.take().ok_or(TerminationReason::Other(
                    "FetchInit already consumed".to_string(),
                ))?;

                let response = self.handle_fetch(&fetch_init.req).await?;
                let _ = fetch_init.res_tx.send(response);
                Ok(())
            }
            Event::Task(mut init) => {
                let task_init = init.take().ok_or(TerminationReason::Other(
                    "TaskInit already consumed".to_string(),
                ))?;

                // The WIT world only exposes handle-scheduled(time), so
                // non-schedule sources pass 0.
                let scheduled_time = match &task_init.source {
                    Some(TaskSource::Schedule { time }) => *time,
                    _ => 0,
                };

                match self.handle_scheduled(scheduled_time).await {
                    Ok(()) => {
                        let _ = task_init.res_tx.send(TaskResult::success());
                        Ok(())
                    }
                    Err(e) => {
                        let _ = task_init.res_tx.send(TaskResult::err(e.to_string()));
                        Err(e)
                    }
                }
            }
        }
    }

    /// Handle a fetch event
    async fn handle_fetch(
        &mut self,
        request: &HttpRequest,
    ) -> Result<HttpResponse, TerminationReason> {
        // Create store with state
        let mut store = self.create_store()?;

        // Create linker and add host functions
        let mut linker = Linker::new(&self.engine);

        // Add WASI to the linker
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add WASI to linker: {}", e))
        })?;

        // Add our custom host functions
        Worker::add_to_linker::<_, HasSelf<WasmState>>(&mut linker, |state| state).map_err(
            |e| TerminationReason::InitializationError(format!("Failed to add to linker: {}", e)),
        )?;

        // Instantiate the component
        let worker = Worker::instantiate_async(&mut store, &self.component, &linker)
            .await
            .map_err(|e| {
                TerminationReason::Exception(format!("Failed to instantiate component: {}", e))
            })?;

        // Convert HttpRequest to WIT types
        let wit_request = self.to_wit_request(request)?;

        // Call the handler
        let wit_response = worker
            .openworkers_worker_handler()
            .call_handle_fetch(&mut store, &wit_request)
            .await
            .map_err(|e| TerminationReason::Exception(format!("handle_fetch failed: {}", e)))?;

        // Convert response back
        Ok(self.from_wit_response(wit_response))
    }

    /// Handle a scheduled event
    async fn handle_scheduled(&mut self, time: u64) -> Result<(), TerminationReason> {
        let mut store = self.create_store()?;

        let mut linker = Linker::new(&self.engine);

        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add WASI to linker: {}", e))
        })?;

        Worker::add_to_linker::<_, HasSelf<WasmState>>(&mut linker, |state| state).map_err(
            |e| TerminationReason::InitializationError(format!("Failed to add to linker: {}", e)),
        )?;

        let worker = Worker::instantiate_async(&mut store, &self.component, &linker)
            .await
            .map_err(|e| {
                TerminationReason::Exception(format!("Failed to instantiate component: {}", e))
            })?;

        worker
            .openworkers_worker_handler()
            .call_handle_scheduled(&mut store, time)
            .await
            .map_err(|e| TerminationReason::Exception(format!("handle_scheduled failed: {}", e)))?;

        Ok(())
    }

    /// Create a new store with state
    fn create_store(&self) -> Result<Store<WasmState>, TerminationReason> {
        let state = WasmState::new(self.env.clone(), self.aborted.clone(), self.ops.clone());
        let mut store = Store::new(&self.engine, state);

        // Set fuel for CPU limiting (if enabled)
        if self.limits.max_cpu_time_ms > 0 {
            // Approximate: 1ms ≈ 10000 fuel units (rough estimate)
            let fuel = self.limits.max_cpu_time_ms * 10000;
            store.set_fuel(fuel).ok();
        }

        Ok(store)
    }

    /// Convert HttpRequest to WIT HttpRequest
    ///
    /// Streaming request bodies are rejected: the WIT interface only carries
    /// buffered bodies (`option<list<u8>>`).
    fn to_wit_request(&self, request: &HttpRequest) -> Result<WitHttpRequest, TerminationReason> {
        let method = match request.method {
            openworkers_core::HttpMethod::Get => WitHttpMethod::Get,
            openworkers_core::HttpMethod::Post => WitHttpMethod::Post,
            openworkers_core::HttpMethod::Put => WitHttpMethod::Put,
            openworkers_core::HttpMethod::Delete => WitHttpMethod::Delete,
            openworkers_core::HttpMethod::Patch => WitHttpMethod::Patch,
            openworkers_core::HttpMethod::Head => WitHttpMethod::Head,
            openworkers_core::HttpMethod::Options => WitHttpMethod::Options,
        };

        let headers: Vec<(String, String)> = request
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let body = match &request.body {
            RequestBody::None => None,
            RequestBody::Bytes(b) => Some(b.to_vec()),
            RequestBody::Stream(_) => {
                return Err(TerminationReason::Other(
                    "streaming request bodies are not supported by the WASM runtime".to_string(),
                ));
            }
        };

        Ok(WitHttpRequest {
            method,
            url: request.url.clone(),
            headers,
            body,
        })
    }

    /// Convert WIT HttpResponse to HttpResponse
    fn from_wit_response(&self, response: WitHttpResponse) -> HttpResponse {
        let headers: Vec<(String, String)> = response.headers;

        let body = match response.body {
            None => ResponseBody::None,
            Some(bytes) => ResponseBody::Bytes(bytes::Bytes::from(bytes)),
        };

        HttpResponse {
            status: response.status,
            headers,
            body,
        }
    }

    /// Abort the worker
    pub fn abort(&mut self) {
        self.aborted.store(true, Ordering::SeqCst);
    }
}

// Implement the Worker trait
impl openworkers_core::Worker for WasmWorker {
    async fn new(script: Script, limits: Option<RuntimeLimits>) -> Result<Self, TerminationReason> {
        WasmWorker::new(script, limits, None).await
    }

    async fn exec(&mut self, event: Event) -> Result<(), TerminationReason> {
        WasmWorker::exec(self, event).await
    }

    fn abort(&mut self) {
        WasmWorker::abort(self)
    }
}
