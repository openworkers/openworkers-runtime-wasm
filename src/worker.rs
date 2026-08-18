//! WebAssembly Worker implementation using Wasmtime Component Model
//!
//! HTTP guests implement the standard wasi:http/proxy world; the cron entry
//! point comes from the custom `openworkers:worker/scheduled` interface.

use http_body_util::BodyExt;
use http_body_util::Full;
use openworkers_core::{
    Event, HttpMethod, HttpRequest, HttpResponse, LogLevel, OperationsHandle, RequestBody,
    ResponseBody, RuntimeLimits, Script, TaskResult, TaskSource, TerminationReason, WorkerCode,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use wasmtime::component::{Component, Linker, ResourceTable, bindgen};
use wasmtime::{Config, Engine, ResourceLimiter, Store, Trap, UpdateDeadline};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::p2::HttpResult;
use wasmtime_wasi_http::p2::WasiHttpCtxView;
use wasmtime_wasi_http::p2::WasiHttpHooks;
use wasmtime_wasi_http::p2::WasiHttpView;
use wasmtime_wasi_http::p2::bindings::ProxyPre;
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::{HyperIncomingBody, HyperOutgoingBody};
use wasmtime_wasi_http::p2::types::{
    HostFutureIncomingResponse, IncomingResponse, OutgoingRequestConfig,
};

// The wasi:http side is covered by wasmtime-wasi-http's own Proxy bindings
bindgen!({
    path: "wit",
    world: "scheduled-only",
    exports: { default: async },
});

/// Interval of the background thread driving epoch interruption; also the
/// granularity of wall-clock and abort checks
const EPOCH_TICK: Duration = Duration::from_millis(10);

/// Crude CPU metering: wasmtime charges roughly one fuel per instruction and
/// this assumes 10k instructions per ms. Not calibrated against real hardware.
const FUEL_UNITS_PER_MS: u64 = 10_000;

/// State held by each WASM instance
pub struct WasmState {
    /// WASI context
    wasi: WasiCtx,
    /// wasi:http context
    http: WasiHttpCtx,
    /// Resource table (required by wasmtime component model)
    table: ResourceTable,
    /// Abort flag
    aborted: Arc<AtomicBool>,
    /// Wall-clock deadline for this execution (None = no limit)
    deadline: Option<Instant>,
    /// Memory limiter enforcing heap_max_mb
    limiter: MemoryLimiter,
    /// Outbound request routing (through the runner's ops handler)
    hooks: OpsHooks,
}

impl WasmState {
    fn new(
        env: &HashMap<String, String>,
        aborted: Arc<AtomicBool>,
        ops: Option<OperationsHandle>,
        deadline: Option<Instant>,
        max_memory_bytes: usize,
    ) -> Self {
        // Build WASI context with environment variables
        let mut wasi_builder = WasiCtxBuilder::new();

        for (k, v) in env {
            wasi_builder.env(k, v);
        }

        // Guest stdout/stderr are the log channel; without ops they go to
        // the host process output
        match &ops {
            Some(ops) => {
                wasi_builder.stdout(OpsLogStream {
                    level: LogLevel::Info,
                    ops: ops.clone(),
                });
                wasi_builder.stderr(OpsLogStream {
                    level: LogLevel::Error,
                    ops: ops.clone(),
                });
            }
            None => {
                wasi_builder.inherit_stdout();
                wasi_builder.inherit_stderr();
            }
        }

        Self {
            wasi: wasi_builder.build(),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            aborted,
            deadline,
            limiter: MemoryLimiter {
                max_memory_bytes,
                memory_limit_hit: false,
            },
            hooks: OpsHooks { ops },
        }
    }
}

impl WasiView for WasmState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for WasmState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.hooks,
        }
    }
}

/// Per-store memory cap; records when the cap denied a growth so the failure
/// can be reported as MemoryLimit instead of a generic trap
struct MemoryLimiter {
    max_memory_bytes: usize,
    memory_limit_hit: bool,
}

impl ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_memory_bytes {
            self.memory_limit_hit = true;

            return Ok(false);
        }

        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

/// Routes the guest's wasi:http/outgoing-handler calls to the runner
struct OpsHooks {
    ops: Option<OperationsHandle>,
}

impl WasiHttpHooks for OpsHooks {
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let Some(ops) = self.ops.clone() else {
            return Err(ErrorCode::InternalError(Some(
                "fetch not available: no operations handle".to_string(),
            ))
            .into());
        };

        Ok(HostFutureIncomingResponse::pending(
            wasmtime_wasi::runtime::spawn(async move {
                Ok(ops_send_request(ops, request, config).await)
            }),
        ))
    }
}

/// Buffer the outgoing request, run it through ops.handle_fetch, and buffer
/// the response back into a wasi:http incoming response
async fn ops_send_request(
    ops: OperationsHandle,
    request: hyper::Request<HyperOutgoingBody>,
    config: OutgoingRequestConfig,
) -> Result<IncomingResponse, ErrorCode> {
    let (parts, body) = request.into_parts();

    let body_bytes = body
        .collect()
        .await
        .map_err(|e| ErrorCode::InternalError(Some(format!("request body failed: {}", e))))?
        .to_bytes();

    let method: HttpMethod = parts
        .method
        .as_str()
        .parse()
        .map_err(|_| ErrorCode::HttpRequestMethodInvalid)?;

    let mut headers = HashMap::new();

    for (name, value) in &parts.headers {
        if let Ok(value) = value.to_str() {
            headers.insert(name.to_string(), value.to_string());
        }
    }

    let core_request = HttpRequest {
        method,
        url: parts.uri.to_string(),
        headers,
        body: if body_bytes.is_empty() {
            RequestBody::None
        } else {
            RequestBody::Bytes(body_bytes)
        },
    };

    let response = ops
        .handle_fetch(core_request)
        .await
        .map_err(|e| ErrorCode::InternalError(Some(format!("fetch failed: {}", e))))?;

    let mut builder = hyper::Response::builder().status(response.status);

    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }

    let body_bytes = response.body.collect().await.unwrap_or_default();

    let resp = builder
        .body(full_body(body_bytes))
        .map_err(|e| ErrorCode::InternalError(Some(format!("invalid response: {}", e))))?;

    Ok(IncomingResponse {
        resp,
        worker: None,
        between_bytes_timeout: config.between_bytes_timeout,
    })
}

/// A buffered hyper body with the error type wasi:http expects
fn full_body(bytes: bytes::Bytes) -> HyperIncomingBody {
    Full::new(bytes).map_err(|e| match e {}).boxed_unsync()
}

/// Guest stdout/stderr sink forwarding complete lines to the ops handler
struct OpsLogStream {
    level: LogLevel,
    ops: OperationsHandle,
}

impl IsTerminal for OpsLogStream {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for OpsLogStream {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(OpsLogWriter {
            level: self.level,
            ops: self.ops.clone(),
            buffer: Vec::new(),
        })
    }
}

struct OpsLogWriter {
    level: LogLevel,
    ops: OperationsHandle,
    /// Bytes of the current, not yet newline-terminated line
    buffer: Vec<u8>,
}

impl OpsLogWriter {
    fn emit(&self, line: &[u8]) {
        self.ops
            .handle_log(self.level, String::from_utf8_lossy(line).into_owned());
    }
}

impl tokio::io::AsyncWrite for OpsLogWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.buffer.extend_from_slice(buf);

        while let Some(pos) = this.buffer.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = this.buffer.drain(..=pos).collect();
            this.emit(&line[..line.len() - 1]);
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for OpsLogWriter {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.emit(&line);
        }
    }
}

/// WebAssembly Worker using Wasmtime Component Model
pub struct WasmWorker {
    /// Wasmtime engine (can be shared across workers)
    engine: Engine,
    /// Pre-instantiated wasi:http/incoming-handler (None if not exported)
    proxy_pre: Option<ProxyPre<WasmState>>,
    /// Pre-instantiated scheduled handler (None if not exported)
    scheduled_pre: Option<ScheduledOnlyPre<WasmState>>,
    /// Runtime limits
    limits: RuntimeLimits,
    /// Abort flag
    aborted: Arc<AtomicBool>,
    /// Environment variables from script
    env: HashMap<String, String>,
    /// Operations handle for fetch, log, etc. (delegated to runner)
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

        let mut config = Config::new();

        // Epoch interruption drives wall-clock limits and abort()
        config.epoch_interruption(true);

        // Enable fuel-based metering for CPU limiting
        if limits.max_cpu_time_ms > 0 {
            config.consume_fuel(true);
        }

        let engine = Engine::new(&config).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to create engine: {}", e))
        })?;

        // Ticker thread; exits once the engine (and thus the worker) is dropped
        let engine_weak = engine.weak();

        std::thread::spawn(move || {
            loop {
                std::thread::sleep(EPOCH_TICK);

                let Some(engine) = engine_weak.upgrade() else {
                    break;
                };

                engine.increment_epoch();
            }
        });

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

        let mut linker = Linker::new(&engine);

        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to add WASI to linker: {}", e))
        })?;

        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).map_err(|e| {
            TerminationReason::InitializationError(format!(
                "Failed to add wasi:http to linker: {}",
                e
            ))
        })?;

        let instance_pre = linker.instantiate_pre(&component).map_err(|e| {
            TerminationReason::InitializationError(format!(
                "Failed to pre-instantiate component: {}",
                e
            ))
        })?;

        // A guest may export the HTTP handler, the scheduled handler, or both
        let proxy_pre = ProxyPre::new(instance_pre.clone()).ok();
        let scheduled_pre = ScheduledOnlyPre::new(instance_pre).ok();

        if proxy_pre.is_none() && scheduled_pre.is_none() {
            return Err(TerminationReason::InitializationError(
                "component exports neither wasi:http/incoming-handler nor \
                 openworkers:worker/scheduled"
                    .to_string(),
            ));
        }

        Ok(Self {
            engine,
            proxy_pre,
            scheduled_pre,
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

                let response = self.handle_fetch(fetch_init.req).await?;
                let _ = fetch_init.res_tx.send(response);
                Ok(())
            }
            Event::Task(mut init) => {
                let task_init = init.take().ok_or(TerminationReason::Other(
                    "TaskInit already consumed".to_string(),
                ))?;

                // The scheduled export only carries a timestamp, so
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

    /// Handle a fetch event through wasi:http/incoming-handler
    async fn handle_fetch(
        &mut self,
        request: HttpRequest,
    ) -> Result<HttpResponse, TerminationReason> {
        let Some(proxy_pre) = &self.proxy_pre else {
            return Err(TerminationReason::Other(
                "guest does not export wasi:http/incoming-handler".to_string(),
            ));
        };

        let mut store = self.create_store();

        let proxy = match proxy_pre.instantiate_async(&mut store).await {
            Ok(proxy) => proxy,
            Err(e) => return Err(Self::termination_reason(&store, "instantiate", e)),
        };

        let scheme = if request.url.starts_with("https://") {
            Scheme::Https
        } else {
            Scheme::Http
        };

        let mut builder = hyper::Request::builder()
            .method(request.method.as_str())
            .uri(&request.url);

        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }

        // Streaming request bodies are buffered here; pass-through streaming
        // is not implemented yet
        let body_bytes = request.body.collect().await.unwrap_or_default();

        let hyper_request = builder.body(full_body(body_bytes)).map_err(|e| {
            TerminationReason::Other(format!("could not build guest request: {}", e))
        })?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let (guest_request, guest_response_out) = {
            let mut http = store.data_mut().http();

            let guest_request = http
                .new_incoming_request(scheme, hyper_request)
                .map_err(|e| {
                    TerminationReason::Other(format!("could not create guest request: {}", e))
                })?;

            let guest_response_out = http.new_response_outparam(response_tx).map_err(|e| {
                TerminationReason::Other(format!("could not create response channel: {}", e))
            })?;

            (guest_request, guest_response_out)
        };

        // Run the guest concurrently with response collection: the guest may
        // still be streaming the body when the response head arrives
        let guest_task = wasmtime_wasi::runtime::spawn(async move {
            let result = proxy
                .wasi_http_incoming_handler()
                .call_handle(&mut store, guest_request, guest_response_out)
                .await;

            result.map_err(|e| Self::termination_reason(&store, "handle", e))
        });

        match response_rx.await {
            Ok(Ok(response)) => {
                let (parts, body) = response.into_parts();

                let mut headers = Vec::new();

                for (name, value) in &parts.headers {
                    if let Ok(value) = value.to_str() {
                        headers.push((name.to_string(), value.to_string()));
                    }
                }

                let body = match body.collect().await {
                    Ok(collected) => {
                        let bytes = collected.to_bytes();

                        if bytes.is_empty() {
                            ResponseBody::None
                        } else {
                            ResponseBody::Bytes(bytes)
                        }
                    }
                    Err(e) => {
                        return Err(match guest_task.await {
                            Err(reason) => reason,
                            Ok(()) => {
                                TerminationReason::Exception(format!("response body failed: {}", e))
                            }
                        });
                    }
                };

                Ok(HttpResponse {
                    status: parts.status.as_u16(),
                    headers,
                    body,
                })
            }
            Ok(Err(code)) => Err(TerminationReason::Exception(format!(
                "guest rejected request: {}",
                code
            ))),
            Err(_) => match guest_task.await {
                Err(reason) => Err(reason),
                Ok(()) => Err(TerminationReason::Exception(
                    "guest returned without producing a response".to_string(),
                )),
            },
        }
    }

    /// Handle a scheduled event
    async fn handle_scheduled(&mut self, time: u64) -> Result<(), TerminationReason> {
        let Some(scheduled_pre) = &self.scheduled_pre else {
            return Err(TerminationReason::Other(
                "guest does not export openworkers:worker/scheduled".to_string(),
            ));
        };

        let mut store = self.create_store();

        let guest = match scheduled_pre.instantiate_async(&mut store).await {
            Ok(guest) => guest,
            Err(e) => return Err(Self::termination_reason(&store, "instantiate", e)),
        };

        let call_result = guest
            .openworkers_worker_scheduled()
            .call_handle_scheduled(&mut store, time)
            .await;

        call_result.map_err(|e| Self::termination_reason(&store, "handle_scheduled", e))?;

        Ok(())
    }

    /// Create a fresh store with limits armed
    fn create_store(&self) -> Store<WasmState> {
        let deadline = (self.limits.max_wall_clock_time_ms > 0)
            .then(|| Instant::now() + Duration::from_millis(self.limits.max_wall_clock_time_ms));

        let state = WasmState::new(
            &self.env,
            self.aborted.clone(),
            self.ops.clone(),
            deadline,
            self.limits.heap_max_mb * 1024 * 1024,
        );

        let mut store = Store::new(&self.engine, state);

        store.limiter(|state| &mut state.limiter);

        if self.limits.max_cpu_time_ms > 0 {
            store
                .set_fuel(self.limits.max_cpu_time_ms * FUEL_UNITS_PER_MS)
                .ok();
        }

        // Re-check the wall-clock deadline and abort flag on every epoch tick
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(|cx| {
            let state = cx.data();

            if state.aborted.load(Ordering::SeqCst) {
                return Ok(UpdateDeadline::Interrupt);
            }

            if state.deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(UpdateDeadline::Interrupt);
            }

            Ok(UpdateDeadline::Continue(1))
        });

        store
    }

    /// Map a failed guest call to a TerminationReason using the store state
    fn termination_reason(
        store: &Store<WasmState>,
        context: &str,
        e: wasmtime::Error,
    ) -> TerminationReason {
        let state = store.data();

        // A denied memory growth surfaces as a guest allocation failure trap,
        // so the limiter flag has to be checked before the trap code
        if state.limiter.memory_limit_hit {
            return TerminationReason::MemoryLimit;
        }

        if state.aborted.load(Ordering::SeqCst) {
            return TerminationReason::Aborted;
        }

        if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
            return TerminationReason::CpuTimeLimit;
        }

        if state.deadline.is_some_and(|d| Instant::now() >= d) {
            return TerminationReason::WallClockTimeout;
        }

        TerminationReason::Exception(format!("{} failed: {}", context, e))
    }

    /// Abort the worker
    ///
    /// Interrupts running guest code at the next epoch check; the epoch bump
    /// makes that immediate instead of waiting for the next ticker interval.
    pub fn abort(&mut self) {
        self.aborted.store(true, Ordering::SeqCst);
        self.engine.increment_epoch();
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
