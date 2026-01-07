//! WebAssembly Worker implementation using Wasmtime Component Model
//!
//! Uses WIT (WebAssembly Interface Types) for type-safe host/guest communication.

use openworkers_core::{
    HttpRequest, HttpResponse, RequestBody, ResponseBody, RuntimeLimits, Script, Task,
    TerminationReason, WorkerCode,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use wasmtime::component::{bindgen, Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};

// Generate bindings from the WIT file
bindgen!({
    path: "wit/worker.wit",
    async: true,
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
}

impl WasmState {
    fn new(env: HashMap<String, String>, aborted: Arc<AtomicBool>) -> Self {
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
        }
    }
}

// Implement WasiView for WASI support
impl WasiView for WasmState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
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
}

impl WasmWorker {
    /// Create a new WASM worker from a Component
    pub async fn new(
        script: Script,
        limits: Option<RuntimeLimits>,
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
        })
    }

    /// Execute a task
    pub async fn exec(&mut self, task: Task) -> Result<(), TerminationReason> {
        if self.aborted.load(Ordering::SeqCst) {
            return Err(TerminationReason::Aborted);
        }

        match task {
            Task::Fetch(mut init) => {
                let fetch_init = init.take().ok_or(TerminationReason::Other(
                    "FetchInit already consumed".to_string(),
                ))?;

                let response = self.handle_fetch(&fetch_init.req).await?;
                let _ = fetch_init.res_tx.send(response);
                Ok(())
            }
            Task::Scheduled(mut init) => {
                let scheduled_init = init.take().ok_or(TerminationReason::Other(
                    "ScheduledInit already consumed".to_string(),
                ))?;

                self.handle_scheduled(scheduled_init.time).await?;
                let _ = scheduled_init.res_tx.send(());
                Ok(())
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
        wasmtime_wasi::add_to_linker_async(&mut linker).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add WASI to linker: {}", e))
        })?;

        // Add our custom host functions
        Worker::add_to_linker(&mut linker, |state: &mut WasmState| state).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add to linker: {}", e))
        })?;

        // Instantiate the component
        let worker = Worker::instantiate_async(&mut store, &self.component, &linker)
            .await
            .map_err(|e| {
                TerminationReason::Exception(format!("Failed to instantiate component: {}", e))
            })?;

        // Convert HttpRequest to WIT types
        let wit_request = self.to_wit_request(request);

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

        wasmtime_wasi::add_to_linker_async(&mut linker).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add WASI to linker: {}", e))
        })?;

        Worker::add_to_linker(&mut linker, |state: &mut WasmState| state).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add to linker: {}", e))
        })?;

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
        let state = WasmState::new(self.env.clone(), self.aborted.clone());
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
    fn to_wit_request(&self, request: &HttpRequest) -> WitHttpRequest {
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
        };

        WitHttpRequest {
            method,
            url: request.url.clone(),
            headers,
            body,
        }
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
        WasmWorker::new(script, limits).await
    }

    async fn exec(&mut self, task: Task) -> Result<(), TerminationReason> {
        WasmWorker::exec(self, task).await
    }

    fn abort(&mut self) {
        WasmWorker::abort(self)
    }
}
