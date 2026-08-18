//! Integration test for hello-worker WASM module

use openworkers_core::{Event, HttpMethod, HttpRequest, RequestBody, Script, WorkerCode};
use openworkers_runtime_wasm::WasmWorker;
use std::collections::HashMap;

/// Load the hello-worker WASM component
fn load_hello_worker_wasm() -> Vec<u8> {
    let wasm_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/hello-worker/target/wasm32-wasip2/release/hello_worker.wasm"
    );

    std::fs::read(wasm_path).expect(&format!(
        "Failed to read WASM file. Build it first with:\n\
         cd examples/hello-worker && cargo build --target wasm32-wasip2 --release"
    ))
}

#[tokio::test]
async fn test_hello_worker_fetch() {
    let wasm_bytes = load_hello_worker_wasm();

    // Create script with WASM bytes
    let script = Script {
        code: WorkerCode::WebAssembly(wasm_bytes),
        env: Some(HashMap::from([(
            "GREETING".to_string(),
            "Bonjour".to_string(),
        )])),
        bindings: vec![],
    };

    // Create worker (no ops handle, so fetch won't work)
    let mut worker = WasmWorker::new(script, None, None)
        .await
        .expect("Failed to create worker");

    // Create request
    let request = HttpRequest {
        url: "https://example.com/test".to_string(),
        method: HttpMethod::Get,
        headers: HashMap::from([("User-Agent".to_string(), "OpenWorkers-Test".to_string())]),
        body: RequestBody::None,
    };

    // Execute event
    let (event, rx) = Event::fetch(request);
    worker.exec(event).await.expect("Failed to execute event");

    // Get response
    let response = rx.await.expect("Failed to receive response");

    println!("Status: {}", response.status);
    println!("Headers: {:?}", response.headers);

    if let openworkers_core::ResponseBody::Bytes(body) = &response.body {
        println!("Body: {}", String::from_utf8_lossy(body));
    }

    assert_eq!(response.status, 200);

    // Check body contains our greeting
    if let openworkers_core::ResponseBody::Bytes(body) = &response.body {
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

    if let openworkers_core::ResponseBody::Bytes(body) = &response.body {
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

    // Execute scheduled task
    let (event, rx) = Event::from_schedule("test-task".to_string(), 1234567890);
    worker.exec(event).await.expect("Failed to execute event");

    // Should complete successfully
    let result = rx.await.expect("Scheduled task should complete");
    assert!(result.success, "Task result should be successful");
}
