//! Integration test for hello-worker WASM module

use openworkers_core::{
    DatabaseOp, DatabaseResult, Event, HttpMethod, HttpRequest, HttpResponse, KvOp, KvResult,
    LogLevel, OpFuture, OperationsHandler, RequestBody, ResponseBody, RuntimeLimits, Script,
    SqlParam, SqlPrimitive, StorageOp, StorageResult, TerminationReason, WorkerCode,
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

const SPIN_BUDGET_MS: u64 = 500;

#[tokio::test]
async fn test_spinning_guest_does_not_block_its_neighbour() {
    let make_script = || Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let spin_limits = RuntimeLimits {
        max_cpu_time_ms: 0,
        max_wall_clock_time_ms: SPIN_BUDGET_MS,
        ..Default::default()
    };

    let mut spinner = WasmWorker::new(make_script(), Some(spin_limits), None)
        .await
        .expect("Failed to create worker");

    let mut neighbour = WasmWorker::new(make_script(), None, None)
        .await
        .expect("Failed to create worker");

    let start = std::time::Instant::now();

    let spin = async {
        let (event, _rx) = Event::fetch(get_request("https://example.com/spin"));
        spinner.exec(event).await
    };

    let serve = async {
        let (event, rx) = Event::fetch(get_request("https://example.com/hello"));
        neighbour
            .exec(event)
            .await
            .expect("Failed to execute event");
        rx.await.expect("Failed to receive response");
        start.elapsed()
    };

    let (spin_result, served_after) = tokio::join!(spin, serve);

    assert_eq!(spin_result, Err(TerminationReason::WallClockTimeout));
    assert!(
        served_after < std::time::Duration::from_millis(SPIN_BUDGET_MS / 2),
        "the spinning guest held the executor for {:?}",
        served_after
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

/// The default 50 ms budget has to buy 50 ms of work: at the uncalibrated
/// 10k fuel per ms it bought about 20 us and this page never finished
#[tokio::test]
async fn test_default_cpu_budget_renders_a_page() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/render"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);
    assert!(body_text(&response).ends_with("<li>item 19999 of 20000</li></ul>"));
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

/// A component targeting `world fetch-worker` serves HTTP with the platform
/// bindings linked
#[tokio::test]
async fn test_fetch_worker_component_serves_http() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_component("fetch-worker")),
        env: None,
        bindings: vec![],
    };

    let ops = Arc::new(BindingOps::default());

    let mut worker = WasmWorker::new_with_ops(script, None, ops)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 200);
    assert_eq!(body_text(&response), r#""hello""#);
}

/// `world fetch-worker` has no scheduled export, so cron events are refused
#[tokio::test]
async fn test_fetch_worker_component_has_no_scheduled_handler() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_component("fetch-worker")),
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

/// Stands in for the runner's binding handlers: it records the SQL it was
/// given and keeps KV and storage in memory so values can be read back
#[derive(Default)]
struct BindingOps {
    queries: Mutex<Vec<(String, String, Vec<SqlParam>)>>,
    values: Mutex<HashMap<String, serde_json::Value>>,
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl OperationsHandler for BindingOps {
    fn handle_binding_database(
        &self,
        binding: &str,
        op: DatabaseOp,
    ) -> OpFuture<'_, DatabaseResult> {
        let DatabaseOp::Query { sql, params } = op;

        self.queries
            .lock()
            .unwrap()
            .push((binding.to_string(), sql.clone(), params));

        // The runner answers a row-returning statement with an array and a
        // mutation with its count
        let json = match sql.starts_with("SELECT") {
            true => r#"[{"id":1,"name":"widget"}]"#,
            false => r#"{"rowsAffected":3}"#,
        };

        Box::pin(async move { DatabaseResult::Rows(json.to_string()) })
    }

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
            KvOp::List { prefix, .. } => KvResult::Keys(matching_keys(values.keys(), prefix)),
        };

        Box::pin(async move { result })
    }

    fn handle_binding_storage(&self, _binding: &str, op: StorageOp) -> OpFuture<'_, StorageResult> {
        let mut objects = self.objects.lock().unwrap();

        let result = match op {
            StorageOp::Get { key } => StorageResult::Body(objects.get(&key).cloned()),
            StorageOp::Put { key, body } => {
                objects.insert(key, body);
                StorageResult::Body(None)
            }
            StorageOp::Delete { key } => {
                objects.remove(&key);
                StorageResult::Body(None)
            }
            StorageOp::Head { key } => match objects.get(&key) {
                Some(body) => StorageResult::Head {
                    size: body.len() as u64,
                    etag: Some("mock-etag".to_string()),
                },
                None => StorageResult::Error("Object not found".to_string()),
            },
            StorageOp::List { prefix, .. } => StorageResult::List {
                keys: matching_keys(objects.keys(), prefix),
                truncated: false,
            },
            StorageOp::Fetch { .. } => StorageResult::Error("fetch is not part of the WIT".into()),
        };

        Box::pin(async move { result })
    }
}

fn matching_keys<'a>(
    keys: impl Iterator<Item = &'a String>,
    prefix: Option<String>,
) -> Vec<String> {
    let prefix = prefix.unwrap_or_default();

    keys.filter(|key| key.starts_with(&prefix))
        .cloned()
        .collect()
}

async fn serve_with_bindings(path: &str) -> (HttpResponse, Arc<BindingOps>) {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let ops = Arc::new(BindingOps::default());

    let mut worker = WasmWorker::new_with_ops(script, None, ops.clone())
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request(&format!("https://example.com{}", path)));
    worker.exec(event).await.expect("Failed to execute event");

    (rx.await.expect("Failed to receive response"), ops)
}

fn body_text(response: &HttpResponse) -> String {
    let ResponseBody::Bytes(body) = &response.body else {
        panic!("Expected Bytes response body");
    };

    String::from_utf8_lossy(body).into_owned()
}

#[tokio::test]
async fn test_database_binding_binds_typed_params() {
    let (response, ops) = serve_with_bindings("/db").await;

    assert_eq!(response.status, 200);
    assert_eq!(
        body_text(&response),
        r#"first={"id":1,"name":"widget"} all=[{"id":1,"name":"widget"}] affected=3 first-rows=1"#
    );

    let queries = ops.queries.lock().unwrap();

    assert_eq!(queries.len(), 3);

    let (binding, sql, params) = &queries[0];

    assert_eq!(binding, "DB");
    assert_eq!(sql, "SELECT * FROM items WHERE id = $1");

    assert!(matches!(
        params[0],
        SqlParam::Primitive(SqlPrimitive::Int(42))
    ));
    assert!(matches!(
        params[1],
        SqlParam::Primitive(SqlPrimitive::Float(f)) if f == 1.5
    ));
    assert!(matches!(
        &params[2],
        SqlParam::Primitive(SqlPrimitive::String(s)) if s == "widget"
    ));
    assert!(matches!(params[3], SqlParam::Primitive(SqlPrimitive::Null)));
    assert!(matches!(
        params[4],
        SqlParam::Primitive(SqlPrimitive::Bool(true))
    ));
    assert!(matches!(
        &params[5],
        SqlParam::Array(values) if values.len() == 2
    ));
}

#[tokio::test]
async fn test_kv_binding_round_trips_values() {
    let (response, ops) = serve_with_bindings("/kv").await;

    assert_eq!(response.status, 200);
    assert_eq!(
        body_text(&response),
        r#"value="hello" keys=greeting deleted=true"#
    );

    assert!(ops.values.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_storage_binding_round_trips_bytes() {
    let (response, ops) = serve_with_bindings("/storage-bytes").await;

    assert_eq!(response.status, 200);

    let expected: Vec<u8> = (0..=u8::MAX).collect();

    let ResponseBody::Bytes(body) = &response.body else {
        panic!("Expected Bytes response body");
    };

    assert_eq!(&body[..], &expected[..]);
    assert_eq!(ops.objects.lock().unwrap()["blob.bin"], expected);
}

#[tokio::test]
async fn test_storage_binding_reports_metadata() {
    let (response, _) = serve_with_bindings("/storage-meta").await;

    assert_eq!(response.status, 200);
    assert_eq!(
        body_text(&response),
        "size=256 etag=mock-etag keys=meta.bin truncated=false"
    );
}

/// Without an operations handle there is nothing to route a binding call to,
/// and the guest sees the error rather than a trap
#[tokio::test]
async fn test_binding_without_ops_reports_the_missing_handle() {
    let script = Script {
        code: WorkerCode::WebAssembly(load_hello_worker_wasm()),
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    let (event, rx) = Event::fetch(get_request("https://example.com/db"));
    worker.exec(event).await.expect("Failed to execute event");

    let response = rx.await.expect("Failed to receive response");

    assert_eq!(response.status, 500);
    assert!(
        body_text(&response).contains("no operations handle"),
        "got: {}",
        body_text(&response)
    );
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
