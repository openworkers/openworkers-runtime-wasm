//! Integration tests for the WASI 0.3 `fetch-worker-v3` world, driven by the
//! fetch-worker-v3 example. They cover what 0.2 cannot do: work that
//! completes after the response, and bodies that flow through a guest whose
//! memory could never hold them whole.

use openworkers_core::{
    Event, HttpMethod, HttpRequest, HttpResponse, KvOp, KvResult, LogLevel, OpFuture,
    OperationsHandler, RequestBody, ResponseBody, RuntimeLimits, Script, TerminationReason,
    WorkerCode,
};
use openworkers_runtime_wasm::WasmWorker;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Load the component built by the given example crate
fn load_component(example: &str) -> Vec<u8> {
    let path = format!(
        "{}/examples/{example}/target/wasm32-wasip2/release/{}.wasm",
        env!("CARGO_MANIFEST_DIR"),
        example.replace('-', "_")
    );

    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "could not read {path}: {e}\n\
             build it with: cd examples/{example} && cargo build --target wasm32-wasip2 --release"
        )
    })
}

fn v3_script() -> Script {
    Script {
        code: WorkerCode::WebAssembly(load_component("fetch-worker-v3")),
        env: None,
        bindings: vec![],
    }
}

fn get_request(url: &str) -> HttpRequest {
    HttpRequest {
        url: url.to_string(),
        method: HttpMethod::Get,
        headers: HashMap::new(),
        body: RequestBody::None,
    }
}

fn body_bytes(response: &HttpResponse) -> &[u8] {
    match &response.body {
        ResponseBody::Bytes(bytes) => bytes,
        other => panic!("expected a buffered body, got {other:?}"),
    }
}

/// Stands in for the runner: kv in memory, upstream fetches recorded
#[derive(Default)]
struct V3Ops {
    values: Mutex<HashMap<String, serde_json::Value>>,
    fetched_urls: Mutex<Vec<String>>,
    logs: Mutex<Vec<(LogLevel, String)>>,
}

impl OperationsHandler for V3Ops {
    fn handle_binding_kv(&self, _binding: &str, op: KvOp) -> OpFuture<'_, KvResult> {
        let mut values = self.values.lock().unwrap();

        let result = match op {
            KvOp::Get { key } => KvResult::Value(values.get(&key).cloned()),
            KvOp::Put { key, value, .. } => {
                values.insert(key, value);
                KvResult::Ok
            }
            KvOp::Delete { key } => {
                values.remove(&key);
                KvResult::Ok
            }
            KvOp::List { .. } => KvResult::Keys(vec![]),
        };

        Box::pin(async move { result })
    }

    fn handle_fetch(&self, request: HttpRequest) -> OpFuture<'_, Result<HttpResponse, String>> {
        self.fetched_urls.lock().unwrap().push(request.url);

        Box::pin(async {
            Ok(HttpResponse {
                status: 200,
                headers: vec![],
                body: ResponseBody::Bytes(bytes::Bytes::from("mock upstream body")),
            })
        })
    }

    fn handle_log(&self, level: LogLevel, message: String) {
        self.logs.lock().unwrap().push((level, message));
    }
}

#[tokio::test]
async fn test_v3_component_serves_http() {
    let mut worker = WasmWorker::new(v3_script(), None, None)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/hello"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);
    assert_eq!(body_bytes(&response), b"hello from v3");
}

#[tokio::test]
async fn test_v3_component_has_no_scheduled_handler() {
    let mut worker = WasmWorker::new(v3_script(), None, None)
        .await
        .expect("Failed to create worker");

    let (event, _rx) = Event::from_schedule("test-task".to_string(), 1234567890);
    let result = worker.exec(event).await;

    assert!(
        matches!(result, Err(TerminationReason::Other(ref message))
            if message.contains("scheduled")),
        "expected the missing scheduled export to be reported, got {result:?}"
    );
}

/// The 0.2 world has no way to run guest code once the response is emitted;
/// here the kv write happens after `handle` returned and the response body
/// was handed over, and it is still there when exec comes back.
#[tokio::test]
async fn test_v3_wait_until_completes_after_response() {
    let ops = Arc::new(V3Ops::default());

    let mut worker = WasmWorker::new_with_ops(v3_script(), None, ops.clone())
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/wait-until"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);
    assert_eq!(body_bytes(&response), b"queued");

    let values = ops.values.lock().unwrap();
    assert_eq!(
        values.get("after-response"),
        Some(&serde_json::json!("done")),
        "the post-response kv write should have completed"
    );
}

/// 32 MiB flow through a guest capped at 8 MiB: the 0.3 body is a stream the
/// guest reads chunk by chunk, so the body size never touches guest memory
#[tokio::test]
async fn test_v3_streams_large_request_through_tight_memory() {
    let limits = RuntimeLimits {
        heap_max_mb: 8,
        max_cpu_time_ms: 0,
        ..Default::default()
    };

    let mut worker = WasmWorker::new(v3_script(), Some(limits), None)
        .await
        .expect("Failed to create worker");

    let payload = vec![b'a'; 32 * 1024 * 1024];

    let request = HttpRequest {
        url: "https://example.com/consume".to_string(),
        method: HttpMethod::Post,
        headers: HashMap::new(),
        body: RequestBody::Bytes(bytes::Bytes::from(payload)),
    };

    let (event, rx) = Event::fetch(request);
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);
    assert_eq!(
        body_bytes(&response),
        (32 * 1024 * 1024).to_string().as_bytes()
    );
}

/// The same guest emits 32 MiB under the same 8 MiB cap, one chunk at a time
#[tokio::test]
async fn test_v3_streams_large_response_through_tight_memory() {
    let limits = RuntimeLimits {
        heap_max_mb: 8,
        max_cpu_time_ms: 0,
        ..Default::default()
    };

    let mut worker = WasmWorker::new(v3_script(), Some(limits), None)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/generate?mb=32"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);

    let body = body_bytes(&response);
    assert_eq!(body.len(), 32 * 1024 * 1024);
    assert!(body.iter().all(|&b| b == b'x'));
}

/// The counterpart that motivates the two tests above: the 0.2 guest can
/// only take a body as one buffer, so the same payload under the same cap
/// dies on the memory limit
#[tokio::test]
async fn test_02_echo_of_large_body_hits_memory_limit() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_component("hello-worker")),
        env: None,
        bindings: vec![],
    };

    let limits = RuntimeLimits {
        heap_max_mb: 8,
        max_cpu_time_ms: 0,
        ..Default::default()
    };

    let mut worker = WasmWorker::new(script, Some(limits), None)
        .await
        .expect("Failed to create worker");

    let payload = vec![b'a'; 32 * 1024 * 1024];

    let request = HttpRequest {
        url: "https://example.com/echo".to_string(),
        method: HttpMethod::Post,
        headers: HashMap::new(),
        body: RequestBody::Bytes(bytes::Bytes::from(payload)),
    };

    let (event, _rx) = Event::fetch(request);
    let result = worker.exec(event).await;

    assert_eq!(result, Err(TerminationReason::MemoryLimit));
}

#[tokio::test]
async fn test_v3_outbound_fetch_flows_through_ops() {
    let ops = Arc::new(V3Ops::default());

    let mut worker = WasmWorker::new_with_ops(v3_script(), None, ops.clone())
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/fetch"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);
    assert_eq!(body_bytes(&response), b"upstream said: mock upstream body");

    let fetched = ops.fetched_urls.lock().unwrap();
    assert_eq!(fetched.as_slice(), ["https://upstream.example/data"]);
}

/// Reports per-request exec latency on the 0.3 path; run with --nocapture
#[tokio::test]
async fn test_v3_exec_latency_report() {
    let mut worker = WasmWorker::new(v3_script(), None, None)
        .await
        .expect("Failed to create worker");

    // Warmup
    for _ in 0..20 {
        let (event, rx) = Event::fetch(get_request("https://example.com/hello"));
        worker.exec(event).await.expect("Failed to execute event");
        rx.await.expect("Failed to receive response");
    }

    const ITERATIONS: u32 = 200;

    let start = std::time::Instant::now();

    for _ in 0..ITERATIONS {
        let (event, rx) = Event::fetch(get_request("https://example.com/hello"));
        worker.exec(event).await.expect("Failed to execute event");
        rx.await.expect("Failed to receive response");
    }

    let elapsed = start.elapsed();

    println!(
        "v3 exec latency: {} iterations in {:?}, avg {:?}/request",
        ITERATIONS,
        elapsed,
        elapsed / ITERATIONS
    );
}
