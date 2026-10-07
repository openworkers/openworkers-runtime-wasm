//! WebAssembly Worker implementation using Wasmtime Component Model
//!
//! HTTP guests implement the standard wasi:http/proxy world; the cron entry
//! point comes from the custom `openworkers:worker/scheduled` interface.

use crate::bindings::WorkerHostPre;
use crate::bindings::task::TaskHostPre;
use crate::bindings::task::exports::openworkers::worker::task as wit_task;
use crate::fuel;
use crate::precompile::PrecompiledComponent;
use crate::precompile::check_wasm_magic;
use crate::shared;
use http_body_util::BodyExt;
use http_body_util::Full;
use openworkers_core::{
    Event, HttpMethod, HttpRequest, HttpResponse, LogLevel, OperationsHandle, RequestBody,
    ResponseBody, RuntimeLimits, Script, TaskInit, TaskResult, TaskSource, TerminationReason,
    WorkerCode,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use wasmtime::component::{Component, ResourceTable};
use wasmtime::{Config, Engine, ResourceLimiter, Store, Trap, UpdateDeadline};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::Error as HttpError;
use wasmtime_wasi_http::RequestOptions;
use wasmtime_wasi_http::WasiBody;
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::WasiHttpCtxView;
use wasmtime_wasi_http::WasiHttpHooks;
use wasmtime_wasi_http::WasiHttpView;
use wasmtime_wasi_http::p2::bindings::ProxyPre;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p3::Request as P3Request;
use wasmtime_wasi_http::p3::bindings::ServicePre;

/// Length at which an unterminated guest log line is emitted anyway
const MAX_LOG_LINE: usize = 8 * 1024;

/// Engine settings, shared by execution and precompilation.
///
/// An artifact records the settings it was compiled with and only loads into
/// an engine that matches, so this is the one place they may be decided.
pub(crate) fn engine_config(limits: &RuntimeLimits) -> Config {
    let mut config = Config::new();

    // Epoch interruption drives wall-clock limits and abort()
    config.epoch_interruption(true);

    // The 0.3 async canonical ABI; inert for 0.2 guests
    config.wasm_component_model_async(true);

    if limits.max_cpu_time_ms > 0 {
        config.consume_fuel(true);
    }

    config
}

/// Where a worker's component comes from.
///
/// The two arms are the whole security boundary: guest bytes are compiled and
/// validated, artifacts are trusted and merely mapped in. Nothing infers the
/// arm from the bytes themselves, or a tenant uploading an artifact-shaped
/// blob would pick the trusted one.
enum ComponentSource<'a> {
    /// Guest-supplied WebAssembly, compiled here
    Wasm(&'a [u8]),
    /// Output of `crate::precompile`, loaded as is
    Precompiled(&'a PrecompiledComponent),
}

pub(crate) struct WasmState {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    aborted: Arc<AtomicBool>,
    /// Wall-clock deadline for this execution; None means no limit
    deadline: Option<Instant>,
    limiter: MemoryLimiter,
    hooks: OpsHooks,
    ops: Option<OperationsHandle>,
}

impl WasmState {
    fn new(
        env: &HashMap<String, String>,
        aborted: Arc<AtomicBool>,
        ops: Option<OperationsHandle>,
        deadline: Option<Instant>,
        max_memory_bytes: Option<usize>,
    ) -> Self {
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
            hooks: OpsHooks { ops: ops.clone() },
            ops,
        }
    }

    /// The runner serves every binding call, so without it there is nothing
    /// to call
    pub(crate) fn ops(&self) -> Result<OperationsHandle, String> {
        self.ops
            .clone()
            .ok_or_else(|| "bindings not available: no operations handle".to_string())
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

/// Per-store memory cap, none when the worker has no limit; records when the
/// cap denied a growth so the failure can be reported as MemoryLimit instead
/// of a generic trap
struct MemoryLimiter {
    max_memory_bytes: Option<usize>,
    memory_limit_hit: bool,
}

impl ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if self.max_memory_bytes.is_some_and(|max| desired > max) {
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

/// Completion future the host interfaces pair with a body
type Done = Box<dyn Future<Output = Result<(), HttpError>> + Send>;

impl WasiHttpHooks for OpsHooks {
    fn send_request(
        &mut self,
        request: hyper::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _fut: Done,
    ) -> Box<dyn Future<Output = Result<(hyper::Response<WasiBody>, Done), HttpError>> + Send> {
        let ops = self.ops.clone();

        Box::new(async move {
            let Some(ops) = ops else {
                return Err(HttpError::InternalError(Some(
                    "fetch not available: no operations handle".to_string(),
                )));
            };

            let response = ops_send_request(ops, request).await?;

            let done: Done = Box::new(std::future::ready(Ok(())));

            Ok((response, done))
        })
    }
}

/// Request and response are buffered whole; the ops handler has no streaming form
async fn ops_send_request(
    ops: OperationsHandle,
    request: hyper::Request<WasiBody>,
) -> Result<hyper::Response<WasiBody>, HttpError> {
    let (parts, body) = request.into_parts();

    let body_bytes = body
        .collect()
        .await
        .map_err(|e| HttpError::InternalError(Some(format!("request body failed: {}", e))))?
        .to_bytes();

    let method: HttpMethod = parts
        .method
        .as_str()
        .parse()
        .map_err(|_| HttpError::HttpRequestMethodInvalid)?;

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
        .map_err(|e| HttpError::InternalError(Some(format!("fetch failed: {}", e))))?;

    let mut builder = hyper::Response::builder().status(response.status);

    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }

    let body_bytes = response
        .body
        .collect()
        .await
        .map_err(|e| HttpError::InternalError(Some(format!("response body failed: {}", e))))?
        .unwrap_or_default();

    builder
        .body(full_body(body_bytes))
        .map_err(|e| HttpError::InternalError(Some(format!("invalid response: {}", e))))
}

/// A buffered hyper body with the error type wasi:http expects
fn full_body(bytes: bytes::Bytes) -> WasiBody {
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

        // Guest output is not covered by the wasm memory limit, so a guest
        // that never writes a newline must not grow this buffer forever
        if this.buffer.len() >= MAX_LOG_LINE {
            let line = std::mem::take(&mut this.buffer);
            this.emit(&line);
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

/// The shareable, per-version part of a worker: the compiled component and
/// its pre-instantiations against the process-wide linker.
///
/// Building one costs the compile (or artifact load) plus linking; assembling
/// a [`WasmWorker`] from it costs a handful of reference-count bumps. A host
/// serving many requests of one worker version holds one of these and calls
/// [`WasmWorker::from_prepared`] per request.
pub struct PreparedComponent {
    engine: Engine,
    component: Component,
    proxy_pre: Option<ProxyPre<WasmState>>,
    service_pre: Option<ServicePre<WasmState>>,
    scheduled_pre: Option<WorkerHostPre<WasmState>>,
    task_pre: Option<TaskHostPre<WasmState>>,
    fuel: bool,
}

impl PreparedComponent {
    /// Serialize the component, giving the same artifact `crate::precompile`
    /// would have produced for it. The bytes carry the trust contract of
    /// [`PrecompiledComponent`].
    pub fn serialize(&self) -> Result<Vec<u8>, TerminationReason> {
        self.component.serialize().map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to serialize component: {}", e))
        })
    }
}

/// WebAssembly Worker using Wasmtime Component Model
pub struct WasmWorker {
    engine: Engine,
    /// The compiled component, for `serialize_component`. Each `*_pre` below
    /// already holds it, so this is one more handle on the same image rather
    /// than a second copy of it.
    component: Component,
    /// None when the guest does not export wasi:http/incoming-handler
    proxy_pre: Option<ProxyPre<WasmState>>,
    /// None when the guest does not export the 0.3 wasi:http/handler
    service_pre: Option<ServicePre<WasmState>>,
    /// None when the guest does not export openworkers:worker/scheduled
    scheduled_pre: Option<WorkerHostPre<WasmState>>,
    /// None when the guest does not export openworkers:worker/task
    task_pre: Option<TaskHostPre<WasmState>>,
    limits: RuntimeLimits,
    aborted: Arc<AtomicBool>,
    env: HashMap<String, String>,
    ops: Option<OperationsHandle>,
}

impl WasmWorker {
    pub async fn new_with_ops(
        script: Script,
        limits: Option<RuntimeLimits>,
        ops: OperationsHandle,
    ) -> Result<Self, TerminationReason> {
        WasmWorker::new(script, limits, Some(ops)).await
    }

    pub async fn new(
        script: Script,
        limits: Option<RuntimeLimits>,
        ops: Option<OperationsHandle>,
    ) -> Result<Self, TerminationReason> {
        let Script { code, env, .. } = script;

        let wasm_bytes = match &code {
            WorkerCode::WebAssembly(bytes) => bytes,
            WorkerCode::JavaScript(_) => {
                return Err(TerminationReason::InitializationError(
                    "WASM runtime cannot execute JavaScript code".to_string(),
                ));
            }
            WorkerCode::Snapshot(_) => {
                return Err(TerminationReason::InitializationError(
                    "WASM runtime cannot execute snapshots: a precompiled component loads \
                     through WasmWorker::new_precompiled"
                        .to_string(),
                ));
            }
        };

        Self::build(env, limits, ops, ComponentSource::Wasm(wasm_bytes))
    }

    /// Build a worker from an artifact `crate::precompile` produced, skipping
    /// compilation.
    ///
    /// `script.code` is not read; the component comes from `component`, and
    /// only `script.env` and `script.bindings` still apply. `limits` must be
    /// the limits `component` was precompiled under, or the engine refuses the
    /// artifact.
    ///
    /// Whoever built the `PrecompiledComponent` vouched for its bytes: read
    /// its trust contract before calling this.
    pub async fn new_precompiled(
        component: PrecompiledComponent,
        script: Script,
        limits: Option<RuntimeLimits>,
        ops: Option<OperationsHandle>,
    ) -> Result<Self, TerminationReason> {
        Self::build(
            script.env,
            limits,
            ops,
            ComponentSource::Precompiled(&component),
        )
    }

    fn build(
        env: Option<HashMap<String, String>>,
        limits: Option<RuntimeLimits>,
        ops: Option<OperationsHandle>,
        source: ComponentSource<'_>,
    ) -> Result<Self, TerminationReason> {
        let prepared = Self::prepare_source(&limits.clone().unwrap_or_default(), source)?;

        Self::assemble(&prepared, env, limits, ops)
    }

    /// Compile guest bytes into the shareable, per-version part of a worker.
    ///
    /// One `PreparedComponent` serves any number of [`WasmWorker::from_prepared`]
    /// calls, which is where the instantiation cost of a request should live.
    /// `limits` only matters for its CPU budget: fuel metering changes the
    /// emitted code, so a prepared component only assembles under the same
    /// fuel mode.
    pub fn prepare(
        wasm: &[u8],
        limits: Option<RuntimeLimits>,
    ) -> Result<PreparedComponent, TerminationReason> {
        Self::prepare_source(&limits.unwrap_or_default(), ComponentSource::Wasm(wasm))
    }

    /// Load an artifact `crate::precompile` produced into the shareable,
    /// per-version part of a worker.
    ///
    /// Whoever built the `PrecompiledComponent` vouched for its bytes: read
    /// its trust contract before calling this.
    pub fn prepare_precompiled(
        component: &PrecompiledComponent,
        limits: Option<RuntimeLimits>,
    ) -> Result<PreparedComponent, TerminationReason> {
        Self::prepare_source(
            &limits.unwrap_or_default(),
            ComponentSource::Precompiled(component),
        )
    }

    /// Assemble a worker from a prepared component, skipping compilation and
    /// linking; the per-request path.
    pub async fn from_prepared(
        prepared: &PreparedComponent,
        script: Script,
        limits: Option<RuntimeLimits>,
        ops: Option<OperationsHandle>,
    ) -> Result<Self, TerminationReason> {
        Self::assemble(prepared, script.env, limits, ops)
    }

    fn prepare_source(
        limits: &RuntimeLimits,
        source: ComponentSource<'_>,
    ) -> Result<PreparedComponent, TerminationReason> {
        let fuel = shared::metered(limits);
        let engine = shared::engine(fuel)?;
        let component = Self::load_component(&engine, source)?;

        let instance_pre = shared::linker(fuel)?
            .instantiate_pre(&component)
            .map_err(|e| {
                TerminationReason::InitializationError(format!(
                    "Failed to pre-instantiate component: {}",
                    e
                ))
            })?;

        // A guest may export either generation of the HTTP handler, the
        // scheduled handler, or a combination; a guest that binds none of
        // them needs every error to be diagnosable
        let proxy_pre = ProxyPre::new(instance_pre.clone());
        let service_pre = ServicePre::new(instance_pre.clone());
        let task_pre = TaskHostPre::new(instance_pre.clone());
        let scheduled_pre = WorkerHostPre::new(instance_pre);

        if let (Err(http), Err(v3), Err(scheduled), Err(task)) =
            (&proxy_pre, &service_pre, &scheduled_pre, &task_pre)
        {
            return Err(TerminationReason::InitializationError(format!(
                "component binds neither wasi:http/incoming-handler ({http}), \
                 wasi:http/handler@0.3.0 ({v3}), \
                 openworkers:worker/scheduled ({scheduled}), nor \
                 openworkers:worker/task ({task})"
            )));
        }

        Ok(PreparedComponent {
            engine,
            component,
            proxy_pre: proxy_pre.ok(),
            service_pre: service_pre.ok(),
            scheduled_pre: scheduled_pre.ok(),
            task_pre: task_pre.ok(),
            fuel,
        })
    }

    fn assemble(
        prepared: &PreparedComponent,
        env: Option<HashMap<String, String>>,
        limits: Option<RuntimeLimits>,
        ops: Option<OperationsHandle>,
    ) -> Result<Self, TerminationReason> {
        let limits = limits.unwrap_or_default();

        if shared::metered(&limits) != prepared.fuel {
            return Err(TerminationReason::InitializationError(
                "prepared component and worker limits disagree on fuel metering".to_string(),
            ));
        }

        Ok(Self {
            engine: prepared.engine.clone(),
            component: prepared.component.clone(),
            proxy_pre: prepared.proxy_pre.clone(),
            service_pre: prepared.service_pre.clone(),
            scheduled_pre: prepared.scheduled_pre.clone(),
            task_pre: prepared.task_pre.clone(),
            limits,
            aborted: Arc::new(AtomicBool::new(false)),
            env: env.unwrap_or_default(),
            ops,
        })
    }

    /// Serialize this worker's component, giving the same artifact
    /// `crate::precompile` would have produced for it.
    ///
    /// A host that just paid for a compile can keep the machine code this way
    /// instead of compiling the guest a second time. The bytes carry the same
    /// trust contract as `crate::precompile` output; see
    /// [`PrecompiledComponent`].
    pub fn serialize_component(&self) -> Result<Vec<u8>, TerminationReason> {
        self.component.serialize().map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to serialize component: {}", e))
        })
    }

    fn load_component(
        engine: &Engine,
        source: ComponentSource<'_>,
    ) -> Result<Component, TerminationReason> {
        match source {
            ComponentSource::Wasm(bytes) => {
                check_wasm_magic(bytes)?;

                Component::new(engine, bytes).map_err(|e| {
                    TerminationReason::InitializationError(format!(
                        "Failed to compile component: {}",
                        e
                    ))
                })
            }
            ComponentSource::Precompiled(component) => {
                // SAFETY: whoever built the PrecompiledComponent vouched for
                // these bytes coming from `crate::precompile`; see its trust
                // contract. A mismatched engine configuration is caught here
                // and reported as an error.
                unsafe { Component::deserialize(engine, component.as_bytes()) }.map_err(|e| {
                    TerminationReason::InitializationError(format!(
                        "Failed to load precompiled component: {}",
                        e
                    ))
                })
            }
        }
    }

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

                if self.task_pre.is_some() {
                    return match self.handle_task(&task_init).await {
                        Ok(result) => {
                            let _ = task_init.res_tx.send(result);
                            Ok(())
                        }
                        Err(e) => {
                            let _ = task_init.res_tx.send(TaskResult::err(e.to_string()));
                            Err(e)
                        }
                    };
                }

                // The scheduled export only carries a timestamp, so
                // non-schedule sources pass 0.
                let scheduled_time = match &task_init.source {
                    Some(TaskSource::Schedule { time, .. }) => *time,
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

    async fn handle_fetch(
        &mut self,
        request: HttpRequest,
    ) -> Result<HttpResponse, TerminationReason> {
        if self.service_pre.is_some() {
            return self.handle_fetch_v3(request).await;
        }

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
        let body_bytes = request
            .body
            .collect()
            .await
            .map_err(|e| TerminationReason::Other(format!("request body failed: {}", e)))?
            .unwrap_or_default();

        let hyper_request = builder.body(full_body(body_bytes)).map_err(|e| {
            TerminationReason::Other(format!("could not build guest request: {}", e))
        })?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let (guest_request, guest_response_out) = {
            let mut http = WasiHttpView::http(store.data_mut());

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

    /// Serves one request through the 0.3 `wasi:http/handler` export.
    ///
    /// Everything stays inside `run_concurrent`, because only its event loop
    /// drives guest tasks. A guest defers post-response work by holding its
    /// trailers future open, so collecting the body is also what runs that
    /// work to completion.
    async fn handle_fetch_v3(
        &mut self,
        request: HttpRequest,
    ) -> Result<HttpResponse, TerminationReason> {
        let Some(service_pre) = &self.service_pre else {
            return Err(TerminationReason::Other(
                "guest does not export wasi:http/handler@0.3.0".to_string(),
            ));
        };

        let mut store = self.create_store();

        let service = match service_pre.instantiate_async(&mut store).await {
            Ok(service) => service,
            Err(e) => return Err(Self::termination_reason(&store, "instantiate", e)),
        };

        let mut builder = hyper::Request::builder()
            .method(request.method.as_str())
            .uri(&request.url);

        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }

        // Buffered at the core boundary; the guest still sees a stream
        let body_bytes = request
            .body
            .collect()
            .await
            .map_err(|e| TerminationReason::Other(format!("request body failed: {}", e)))?
            .unwrap_or_default();

        let hyper_request = builder.body(full_body(body_bytes)).map_err(|e| {
            TerminationReason::Other(format!("could not build guest request: {}", e))
        })?;

        let (guest_request, _request_io) =
            P3Request::from_http(store.data_mut().http().hooks, hyper_request);

        // The epoch deadline only fires while guest code runs, so a guest
        // idling on a handle nothing will complete needs a host-side timeout
        let deadline = store.data().deadline;

        let run = store.run_concurrent(async |accessor| -> wasmtime::Result<_> {
            let response = match service.handle(accessor, guest_request).await? {
                Ok(response) => response,
                Err(code) => return Ok(Err(code)),
            };

            let response = accessor
                .with(|mut access| response.into_http(&mut access, std::future::ready(Ok(()))))?;

            let (parts, body) = response.into_parts();

            let mut headers = Vec::new();

            for (name, value) in &parts.headers {
                if let Ok(value) = value.to_str() {
                    headers.push((name.to_string(), value.to_string()));
                }
            }

            let bytes = body
                .collect()
                .await
                .map_err(|e| wasmtime::format_err!("response body failed: {e}"))?
                .to_bytes();

            Ok(Ok((parts.status.as_u16(), headers, bytes)))
        });

        let result = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline.into(), run).await {
                Ok(result) => result,
                Err(_) => return Err(TerminationReason::WallClockTimeout),
            },
            None => run.await,
        };

        match result {
            Ok(Ok(Ok((status, headers, bytes)))) => Ok(HttpResponse {
                status,
                headers,
                body: if bytes.is_empty() {
                    ResponseBody::None
                } else {
                    ResponseBody::Bytes(bytes)
                },
            }),
            Ok(Ok(Err(code))) => Err(TerminationReason::Exception(format!(
                "guest rejected request: {}",
                code
            ))),
            Ok(Err(e)) | Err(e) => Err(Self::termination_reason(&store, "handle", e)),
        }
    }

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

    /// Runs the task export. The guest's own failure is a failed result, not a
    /// termination: the guest ran to the end.
    async fn handle_task(&mut self, task: &TaskInit) -> Result<TaskResult, TerminationReason> {
        let Some(task_pre) = &self.task_pre else {
            return Err(TerminationReason::Other(
                "guest does not export openworkers:worker/task".to_string(),
            ));
        };

        let event = wit_task::TaskEvent {
            task_id: task.task_id.clone(),
            attempt: task.attempt,
            payload: task.payload.as_ref().map(|payload| payload.to_string()),
            source: task.source.as_ref().map(task_source),
        };

        let mut store = self.create_store();

        let guest = match task_pre.instantiate_async(&mut store).await {
            Ok(guest) => guest,
            Err(e) => return Err(Self::termination_reason(&store, "instantiate", e)),
        };

        let answer = guest
            .openworkers_worker_task()
            .call_handle_task(&mut store, &event)
            .await
            .map_err(|e| Self::termination_reason(&store, "handle_task", e))?;

        Ok(match answer {
            Ok(None) => TaskResult::success(),
            Ok(Some(json)) => match serde_json::from_str(&json) {
                Ok(data) => TaskResult::ok(Some(data)),
                Err(e) => TaskResult::err(format!("the task result is not JSON: {e}")),
            },
            Err(message) => TaskResult::err(message),
        })
    }

    /// Create a fresh store with limits armed
    fn create_store(&self) -> Store<WasmState> {
        let deadline = (self.limits.max_wall_clock_time_ms > 0)
            .then(|| Instant::now() + Duration::from_millis(self.limits.max_wall_clock_time_ms));

        // 0 disables the cap, as it does for the CPU and wall-clock budgets
        let max_memory_bytes =
            (self.limits.heap_max_mb > 0).then(|| self.limits.heap_max_mb * 1024 * 1024);

        let state = WasmState::new(
            &self.env,
            self.aborted.clone(),
            self.ops.clone(),
            deadline,
            max_memory_bytes,
        );

        let mut store = Store::new(&self.engine, state);

        store.limiter(|state| &mut state.limiter);

        if self.limits.max_cpu_time_ms > 0 {
            store
                .set_fuel(self.limits.max_cpu_time_ms.saturating_mul(fuel::per_ms()))
                .ok();
        }

        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(|cx| {
            let state = cx.data();

            if state.aborted.load(Ordering::SeqCst) {
                return Ok(UpdateDeadline::Interrupt);
            }

            if state.deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(UpdateDeadline::Interrupt);
            }

            // Yield, or a compute-bound guest holds the executor until its
            // wall-clock budget expires. UpdateDeadline::Yield wakes itself and
            // lands back in tokio's LIFO slot, so it never reaches the run queue.
            Ok(UpdateDeadline::YieldCustom(
                1,
                Box::pin(tokio::task::yield_now()),
            ))
        });

        store
    }

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

    /// Interrupts running guest code at the next epoch check; the epoch bump
    /// makes that immediate instead of waiting for the next ticker interval.
    pub fn abort(&mut self) {
        self.aborted.store(true, Ordering::SeqCst);
        self.engine.increment_epoch();
    }
}

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

fn task_source(source: &TaskSource) -> wit_task::TaskSource {
    match source {
        TaskSource::Schedule { time, cron } => wit_task::TaskSource::Schedule(wit_task::Schedule {
            time: *time,
            cron: cron.clone(),
        }),
        TaskSource::Chained { parent_task_id, .. } => {
            wit_task::TaskSource::Chained(parent_task_id.clone())
        }
        TaskSource::Worker { worker_id, .. } => wit_task::TaskSource::Worker(worker_id.clone()),
        TaskSource::Invoke { origin } => wit_task::TaskSource::Invoke(origin.clone()),
    }
}
