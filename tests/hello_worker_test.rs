//! Integration test for hello-worker WASM module

use openworkers_core::{HttpMethod, HttpRequest, RequestBody, Script, Task};
use openworkers_runtime_wasm::WasmWorker;
use std::collections::HashMap;

/// Load the hello-worker WASM binary
fn load_hello_worker_wasm() -> Vec<u8> {
    let wasm_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/hello-worker/target/wasm32-unknown-unknown/release/hello_worker.wasm"
    );

    std::fs::read(wasm_path).expect(&format!(
        "Failed to read WASM file. Build it first with:\n\
         cd examples/hello-worker && cargo build --target wasm32-unknown-unknown --release"
    ))
}

#[tokio::test]
async fn test_hello_worker_fetch() {
    let wasm_bytes = load_hello_worker_wasm();

    // Create script with WASM bytes (raw bytes work because of magic number detection)
    let script = Script {
        code: unsafe { String::from_utf8_unchecked(wasm_bytes) },
        env: Some(HashMap::from([(
            "GREETING".to_string(),
            "Bonjour".to_string(),
        )])),
        bindings: vec![],
    };

    // Create worker
    let mut worker = WasmWorker::new(script, None)
        .await
        .expect("Failed to create worker");

    // Create request
    let request = HttpRequest {
        url: "https://example.com/test".to_string(),
        method: HttpMethod::Get,
        headers: HashMap::from([("User-Agent".to_string(), "OpenWorkers-Test".to_string())]),
        body: RequestBody::None,
    };

    // Execute task
    let (task, rx) = Task::fetch(request);
    worker.exec(task).await.expect("Failed to execute task");

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
async fn test_hello_worker_scheduled() {
    let wasm_bytes = load_hello_worker_wasm();

    let script = Script {
        code: unsafe { String::from_utf8_unchecked(wasm_bytes) },
        env: None,
        bindings: vec![],
    };

    let mut worker = WasmWorker::new(script, None)
        .await
        .expect("Failed to create worker");

    // Execute scheduled task
    let (task, rx) = Task::scheduled(1234567890);
    worker.exec(task).await.expect("Failed to execute task");

    // Should complete successfully
    rx.await.expect("Scheduled task should complete");
}
