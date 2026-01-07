//! WebAssembly Worker implementation using Wasmtime
//!
//! This is a minimal implementation that demonstrates the architecture.
//! WASI and full host bindings will be added incrementally.

use openworkers_core::{
    HttpRequest, HttpResponse, ResponseBody, RuntimeLimits, Script, Task, TerminationReason,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use wasmtime::*;

/// State held by each WASM instance
struct WasmState {
    /// Environment variables
    env: std::collections::HashMap<String, String>,
    /// Abort flag
    aborted: Arc<AtomicBool>,
}

/// WebAssembly Worker using Wasmtime
pub struct WasmWorker {
    /// Wasmtime engine (can be shared across workers)
    engine: Engine,
    /// Compiled module
    module: Module,
    /// Runtime limits
    limits: RuntimeLimits,
    /// Abort flag
    aborted: Arc<AtomicBool>,
    /// Environment variables from script
    env: std::collections::HashMap<String, String>,
}

impl WasmWorker {
    /// Create a new WASM worker
    ///
    /// The script.code should contain base64-encoded WASM bytecode.
    /// In the future, we'll add proper binary support to Script.
    pub async fn new(
        script: Script,
        limits: Option<RuntimeLimits>,
    ) -> Result<Self, TerminationReason> {
        let limits = limits.unwrap_or_default();

        // Configure engine
        let mut config = Config::new();
        config.async_support(true);

        // Enable fuel-based metering for CPU limiting
        if limits.max_cpu_time_ms > 0 {
            config.consume_fuel(true);
        }

        let engine = Engine::new(&config).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to create engine: {}", e))
        })?;

        // Decode WASM bytecode from base64 in script.code
        let wasm_bytes = decode_wasm_from_script(&script)?;

        // Compile the module
        let module = Module::new(&engine, &wasm_bytes).map_err(|e| {
            TerminationReason::InitializationError(format!("Failed to compile WASM: {}", e))
        })?;

        Ok(Self {
            engine,
            module,
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
        // Create a new store with state for this request
        let mut store = self.create_store()?;

        // Create linker with host functions
        let mut linker = Linker::new(&self.engine);
        self.add_host_functions(&mut linker)?;

        // Instantiate the module
        let instance = linker
            .instantiate_async(&mut store, &self.module)
            .await
            .map_err(|e| TerminationReason::Exception(format!("Instantiation failed: {}", e)))?;

        // Serialize request to JSON for the guest
        let request_json = serialize_request(request);

        // Get the handle_fetch export
        let handle_fetch = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "handle_fetch")
            .map_err(|e| {
                TerminationReason::Exception(format!(
                    "WASM module must export 'handle_fetch(ptr: i32, len: i32) -> i32': {}",
                    e
                ))
            })?;

        // Write request to guest memory
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| TerminationReason::Exception("No memory export".to_string()))?;

        let request_bytes = request_json.as_bytes();
        let request_ptr = allocate_guest_memory(&instance, &mut store, request_bytes.len())?;

        memory
            .write(&mut store, request_ptr as usize, request_bytes)
            .map_err(|e| TerminationReason::Exception(format!("Memory write failed: {}", e)))?;

        // Call the handler
        let response_ptr = handle_fetch
            .call_async(&mut store, (request_ptr, request_bytes.len() as i32))
            .await
            .map_err(|e| TerminationReason::Exception(format!("handle_fetch failed: {}", e)))?;

        // Read response from guest memory
        let response = read_guest_response(&instance, &mut store, response_ptr)?;

        Ok(response)
    }

    /// Handle a scheduled event
    async fn handle_scheduled(&mut self, time: u64) -> Result<(), TerminationReason> {
        let mut store = self.create_store()?;

        let mut linker = Linker::new(&self.engine);
        self.add_host_functions(&mut linker)?;

        let instance = linker
            .instantiate_async(&mut store, &self.module)
            .await
            .map_err(|e| TerminationReason::Exception(format!("Instantiation failed: {}", e)))?;

        // Get the handle_scheduled export (optional)
        let handle_scheduled =
            match instance.get_typed_func::<u64, i32>(&mut store, "handle_scheduled") {
                Ok(f) => f,
                Err(_) => return Ok(()), // No scheduled handler, that's fine
            };

        handle_scheduled
            .call_async(&mut store, time)
            .await
            .map_err(|e| TerminationReason::Exception(format!("handle_scheduled failed: {}", e)))?;

        Ok(())
    }

    /// Create a new store with state
    fn create_store(&self) -> Result<Store<WasmState>, TerminationReason> {
        let state = WasmState {
            env: self.env.clone(),
            aborted: self.aborted.clone(),
        };

        let mut store = Store::new(&self.engine, state);

        // Set fuel for CPU limiting (if enabled)
        if self.limits.max_cpu_time_ms > 0 {
            // Approximate: 1ms ≈ 10000 fuel units (rough estimate)
            let fuel = self.limits.max_cpu_time_ms * 10000;
            store.set_fuel(fuel).ok();
        }

        Ok(store)
    }

    /// Add host functions to the linker
    fn add_host_functions(&self, linker: &mut Linker<WasmState>) -> Result<(), TerminationReason> {
        // host_log(level: i32, msg_ptr: i32, msg_len: i32)
        linker
            .func_wrap(
                "env",
                "host_log",
                |mut caller: Caller<'_, WasmState>, level: i32, msg_ptr: i32, msg_len: i32| {
                    // Read message from memory
                    if let Some(memory) = caller.get_export("memory") {
                        if let Some(memory) = memory.into_memory() {
                            let mut buf = vec![0u8; msg_len as usize];
                            if memory.read(&caller, msg_ptr as usize, &mut buf).is_ok() {
                                if let Ok(msg) = String::from_utf8(buf) {
                                    let level_str = match level {
                                        0 => "DEBUG",
                                        1 => "INFO",
                                        2 => "WARN",
                                        3 => "ERROR",
                                        _ => "LOG",
                                    };
                                    println!("[WASM {}] {}", level_str, msg);
                                }
                            }
                        }
                    }
                },
            )
            .map_err(|e| {
                TerminationReason::InitializationError(format!("Failed to add host_log: {}", e))
            })?;

        // host_get_env(key_ptr, key_len, value_ptr, value_max_len) -> actual_len or -1
        linker
            .func_wrap(
                "env",
                "host_get_env",
                |mut caller: Caller<'_, WasmState>,
                 key_ptr: i32,
                 key_len: i32,
                 value_ptr: i32,
                 value_max_len: i32|
                 -> i32 {
                    let memory = match caller.get_export("memory") {
                        Some(e) => match e.into_memory() {
                            Some(m) => m,
                            None => return -1,
                        },
                        None => return -1,
                    };

                    // Read key
                    let mut key_buf = vec![0u8; key_len as usize];
                    if memory
                        .read(&caller, key_ptr as usize, &mut key_buf)
                        .is_err()
                    {
                        return -1;
                    }

                    let key = match String::from_utf8(key_buf) {
                        Ok(k) => k,
                        Err(_) => return -1,
                    };

                    // Get value from env
                    let value = match caller.data().env.get(&key) {
                        Some(v) => v.clone(),
                        None => return -1,
                    };

                    let value_bytes = value.as_bytes();
                    if value_bytes.len() > value_max_len as usize {
                        return -(value_bytes.len() as i32); // Negative = needed size
                    }

                    // Write value to guest memory
                    if memory
                        .write(&mut caller, value_ptr as usize, value_bytes)
                        .is_err()
                    {
                        return -1;
                    }

                    value_bytes.len() as i32
                },
            )
            .map_err(|e| {
                TerminationReason::InitializationError(format!("Failed to add host_get_env: {}", e))
            })?;

        Ok(())
    }

    /// Abort the worker
    pub fn abort(&mut self) {
        self.aborted.store(true, Ordering::SeqCst);
    }
}

/// Serialize HttpRequest to JSON string
fn serialize_request(request: &HttpRequest) -> String {
    let headers: serde_json::Map<String, serde_json::Value> = request
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();

    let body_str = match &request.body {
        openworkers_core::RequestBody::None => String::new(),
        openworkers_core::RequestBody::Bytes(b) => String::from_utf8_lossy(b).to_string(),
    };

    serde_json::json!({
        "url": request.url,
        "method": request.method.as_str(),
        "headers": headers,
        "body": body_str
    })
    .to_string()
}

/// Allocate memory in the guest for writing data
fn allocate_guest_memory(
    instance: &Instance,
    store: &mut Store<WasmState>,
    size: usize,
) -> Result<i32, TerminationReason> {
    // Try to call the guest's allocate function
    match instance.get_typed_func::<i32, i32>(&mut *store, "allocate") {
        Ok(alloc_fn) => alloc_fn
            .call(&mut *store, size as i32)
            .map_err(|e| TerminationReason::Exception(format!("allocate failed: {}", e))),
        Err(_) => {
            // Fallback: use a fixed offset (simple but limited)
            // Guest must have at least 64KB memory for this to work
            Ok(1024) // Start after the first 1KB
        }
    }
}

/// Read response from guest memory
fn read_guest_response(
    instance: &Instance,
    store: &mut Store<WasmState>,
    response_ptr: i32,
) -> Result<HttpResponse, TerminationReason> {
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| TerminationReason::Exception("No memory export".to_string()))?;

    // Read response length (first 4 bytes at response_ptr)
    let mut len_bytes = [0u8; 4];
    memory
        .read(&*store, response_ptr as usize, &mut len_bytes)
        .map_err(|e| TerminationReason::Exception(format!("Failed to read length: {}", e)))?;
    let response_len = u32::from_le_bytes(len_bytes) as usize;

    // Sanity check
    if response_len > 10 * 1024 * 1024 {
        return Err(TerminationReason::Exception(
            "Response too large (>10MB)".to_string(),
        ));
    }

    // Read response JSON
    let mut response_bytes = vec![0u8; response_len];
    memory
        .read(&*store, (response_ptr + 4) as usize, &mut response_bytes)
        .map_err(|e| TerminationReason::Exception(format!("Failed to read response: {}", e)))?;

    // Deserialize response
    let response_json = String::from_utf8(response_bytes)
        .map_err(|e| TerminationReason::Exception(format!("Invalid UTF-8: {}", e)))?;

    // Parse as simplified response format: { status, headers, body }
    let parsed: serde_json::Value = serde_json::from_str(&response_json)
        .map_err(|e| TerminationReason::Exception(format!("Invalid JSON response: {}", e)))?;

    let status = parsed["status"].as_u64().unwrap_or(200) as u16;
    let headers = parsed["headers"]
        .as_object()
        .map(|h| {
            h.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();
    let body = parsed["body"]
        .as_str()
        .map(|s| bytes::Bytes::from(s.to_string()))
        .unwrap_or_default();

    Ok(HttpResponse {
        status,
        headers,
        body: ResponseBody::Bytes(body),
    })
}

/// Decode WASM bytecode from Script
///
/// Supports two formats:
/// 1. Raw WASM bytes (starts with \0asm magic)
/// 2. Base64-encoded WASM (for compatibility with String-based Script)
fn decode_wasm_from_script(script: &Script) -> Result<Vec<u8>, TerminationReason> {
    let code = script.code.as_bytes();

    // Check for WASM magic number: \0asm
    if code.len() >= 4 && code[0..4] == [0x00, 0x61, 0x73, 0x6d] {
        return Ok(code.to_vec());
    }

    // Try base64 decode
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(&script.code)
        .map_err(|e| {
            TerminationReason::InitializationError(format!(
                "Script is not valid WASM or base64: {}",
                e
            ))
        })
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
