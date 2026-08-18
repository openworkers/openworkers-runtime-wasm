//! Integration test for hello-worker WASM module

use openworkers_core::{
    Event, HttpMethod, HttpRequest, HttpResponse, LogLevel, OpFuture, OperationsHandler,
    RequestBody, ResponseBody, RuntimeLimits, Script, TerminationReason, WorkerCode,
};
use openworkers_runtime_wasm::WasmWorker;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

fn get_request(url: &str) -> HttpRequest {
    HttpRequest {
        url: url.to_string(),
        method: HttpMethod::Get,
        headers: HashMap::new(),
        body: RequestBody::None,
    }
}

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

fn load_hello_worker_wasm() -> Vec<u8> {
    load_component("hello-worker")
}

#[tokio::test]
async fn test_hello_worker_fetch() {
    let wasm_bytes = load_hello_worker_wasm();

    let script = Script {
        code: WorkerCode::WebAssembly(wasm_bytes),
        env: Some(HashMap::from([(
            "GREETING".to_string(),
            "Bonjour".to_string(),
        )])),
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let request = HttpRequest {
        url: "https://example.com/test".to_string(),
        method: HttpMethod::Get,
        headers: HashMap::from([("User-Agent".to_string(), "OpenWorkers-Test".to_string())]),
        body: RequestBody::None,
    };

    let (event, rx) = Event::fetch(request);
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    println!("Status: {}", response.status);
    println!("Headers: {:?}", response.headers);

    if let ResponseBody::Bytes(body) = &response.body {
        println!("Body: {}", String::from_utf8_lossy(body));
    }

    assert_eq!(response.status, 200);

    if let ResponseBody::Bytes(body) = &response.body {
        let body_str = String::from_utf8_lossy(body);
        assert!(
            body_str.contains("Bonjour"),
            "Body should contain 'Bonjour'"
        );
        assert!(
            body_str.contains("example.com/test"),
            "Body should contain the URL"
        );
    } else {
        panic!("Expected Bytes response body");
    }
}

#[tokio::test]
async fn test_hello_worker_proxy_without_ops() {
    let wasm_bytes = load_hello_worker_wasm();

    let script = Script {
        code: WorkerCode::WebAssembly(wasm_bytes),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let request = HttpRequest {
        url: "https://example.com/proxy".to_string(),
        method: HttpMethod::Get,
        headers: HashMap::new(),
        body: RequestBody::None,
    };

    let (event, rx) = Event::fetch(request);
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    // Without an operations handle, host.fetch fails and the guest reports it
    assert_eq!(response.status, 502);

    if let ResponseBody::Bytes(body) = &response.body {
        let body_str = String::from_utf8_lossy(body);
        assert!(
            body_str.contains("no operations handle"),
            "Body should surface the host fetch error, got: {}",
            body_str
        );
    } else {
        panic!("Expected Bytes response body");
    }
}

struct MockOps {
    logs: Mutex<Vec<(LogLevel, String)>>,
    fetched_urls: Mutex<Vec<String>>,
}

impl OperationsHandler for MockOps {
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
async fn test_log_and_fetch_flow_through_ops() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let ops = Arc::new(MockOps {
        logs: Mutex::new(vec![]),
        fetched_urls: Mutex::new(vec![]),
    });

    let mut worker = WasmWorker::new_with_ops(script, None, ops.clone())
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/proxy"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);

    if let ResponseBody::Bytes(body) = &response.body {
        assert_eq!(&body[..], b"mock upstream body");
    } else {
        panic!("Expected Bytes response body");
    }

    let fetched = ops.fetched_urls.lock().unwrap();
    assert_eq!(fetched.as_slice(), ["https://upstream.example/data"]);

    let logs = ops.logs.lock().unwrap();
    assert!(
        logs.iter().any(|(level, message)| {
            *level == LogLevel::Info && message.contains("example.com/proxy")
        }),
        "Guest request log should reach the ops handler, got: {:?}",
        logs
    );
}

#[tokio::test]
async fn test_long_guest_log_line_is_split() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let ops = Arc::new(MockOps {
        logs: Mutex::new(vec![]),
        fetched_urls: Mutex::new(vec![]),
    });

    let mut worker = WasmWorker::new_with_ops(script, None, ops.clone())
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/longlog"));
    worker.exec(event).await.expect("Failed to execute event");
    rx.await.expect("Failed to receive response");

    let logs = ops.logs.lock().unwrap();

    let lengths: Vec<usize> = logs
        .iter()
        .filter(|(_, message)| message.starts_with('x'))
        .map(|(_, message)| message.len())
        .collect();

    assert!(
        lengths.len() > 1,
        "a 20k character line should be split, got {:?}",
        lengths
    );
    assert_eq!(lengths.iter().sum::<usize>(), 20_000);
}

#[tokio::test]
async fn test_infinite_loop_hits_wall_clock_limit() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let limits = RuntimeLimits {
        max_cpu_time_ms: 0,
        max_wall_clock_time_ms: 200,
        ..Default::default()
    };

    let mut worker = WasmWorker::new(script, Some(limits), None)
        .await
        .expect("Failed to create worker");

    let start = std::time::Instant::now();
    let (event, _rx) = Event::fetch(get_request("https://example.com/spin"));
    let result = worker.exec(event).await;

    assert_eq!(result, Err(TerminationReason::WallClockTimeout));
    assert!(
        start.elapsed() < std::time::Duration::from_secs(5),
        "Worker should be interrupted promptly, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn test_infinite_loop_hits_cpu_fuel_limit() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let limits = RuntimeLimits {
        max_cpu_time_ms: 50,
        ..Default::default()
    };

    let mut worker = WasmWorker::new(script, Some(limits), None)
        .await
        .expect("Failed to create worker");

    let (event, _rx) = Event::fetch(get_request("https://example.com/spin"));
    let result = worker.exec(event).await;

    assert_eq!(result, Err(TerminationReason::CpuTimeLimit));
}

#[tokio::test]
async fn test_memory_hog_hits_memory_limit() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let limits = RuntimeLimits {
        heap_max_mb: 16,
        max_cpu_time_ms: 0,
        ..Default::default()
    };

    let mut worker = WasmWorker::new(script, Some(limits), None)
        .await
        .expect("Failed to create worker");

    let (event, _rx) = Event::fetch(get_request("https://example.com/alloc"));
    let result = worker.exec(event).await;

    assert_eq!(result, Err(TerminationReason::MemoryLimit));
}

/// Reports per-request exec latency; run with --nocapture to see the numbers
#[tokio::test]
async fn test_exec_latency_report() {
    let wasm_bytes = load_hello_worker_wasm();

    let script = Script {
        code: WorkerCode::WebAssembly(wasm_bytes),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let make_request = || HttpRequest {
        url: "https://example.com/bench".to_string(),
        method: HttpMethod::Get,
        headers: HashMap::new(),
        body: RequestBody::None,
    };

    // Warmup
    for _ in 0..20 {
        let (event, rx) = Event::fetch(make_request());
        worker.exec(event).await.expect("Failed to execute event");
        rx.await.expect("Failed to receive response");
    }

    const ITERATIONS: u32 = 200;

    let start = std::time::Instant::now();

    for _ in 0..ITERATIONS {
        let (event, rx) = Event::fetch(make_request());
        worker.exec(event).await.expect("Failed to execute event");
        rx.await.expect("Failed to receive response");
    }

    let elapsed = start.elapsed();

    println!(
        "exec latency: {} iterations in {:?}, avg {:?}/request",
        ITERATIONS,
        elapsed,
        elapsed / ITERATIONS
    );
}

/// A component built against the unmodified wasi:http/proxy world runs as an
/// HTTP worker, with no OpenWorkers-specific WIT
#[tokio::test]
async fn test_stock_proxy_component_serves_http() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_component("proxy-worker")),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/stock"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);

    let ResponseBody::Bytes(body) = &response.body else {
        panic!("Expected Bytes response body");
    };

    assert_eq!(&body[..], b"stock wasi:http proxy answering /stock");
}

/// A proxy-only component has no scheduled export, so cron events are refused
#[tokio::test]
async fn test_stock_proxy_component_has_no_scheduled_handler() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_component("proxy-worker")),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let (event, _rx) = Event::from_schedule("test-task".to_string(), 1234567890);

    assert!(matches!(
        worker.exec(event).await,
        Err(TerminationReason::Other(_))
    ));
}

#[tokio::test]
async fn test_request_body_reaches_the_guest() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let request = HttpRequest {
        url: "https://example.com/echo".to_string(),
        method: HttpMethod::Post,
        headers: HashMap::new(),
        body: RequestBody::Bytes(bytes::Bytes::from_static(b"round trip")),
    };

    let (event, rx) = Event::fetch(request);
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);

    let ResponseBody::Bytes(body) = &response.body else {
        panic!("Expected Bytes response body");
    };

    assert_eq!(&body[..], b"round trip");
}

#[tokio::test]
async fn test_hello_worker_scheduled() {
    let wasm_bytes = load_hello_worker_wasm();

    let script = Script {
        code: WorkerCode::WebAssembly(wasm_bytes),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::from_schedule("test-task".to_string(), 1234567890);
    worker.exec(event).await.expect("Failed to execute event");

    let result = rx.await.expect("Scheduled task should complete");
    assert!(result.success, "Task result should be successful");
}
