//! One PreparedComponent serves many workers

use openworkers_core::Event;
use openworkers_core::HttpMethod;
use openworkers_core::HttpRequest;
use openworkers_core::RequestBody;
use openworkers_core::RuntimeLimits;
use openworkers_core::Script;
use openworkers_core::TerminationReason;
use openworkers_core::WorkerCode;
use openworkers_runtime_wasm::WasmWorker;
use std::collections::HashMap;

fn load_component(example: &str) -> Vec<u8> {
    let path = format!(
        "{}/examples/{example}/target/wasm32-wasip2/release/{}.wasm",
        env!("CARGO_MANIFEST_DIR"),
        example.replace('-', "_")
    );

    std::fs::read(&path).expect("component not built")
}

fn script() -> Script {
    Script {
        code: WorkerCode::WebAssembly(vec![]),
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

#[tokio::test]
async fn one_prepared_component_serves_many_workers() {
    let prepared =
        WasmWorker::prepare(&load_component("hello-worker"), None).expect("prepare should compile");

    for _ in 0..2 {
        let mut worker = WasmWorker::from_prepared(&prepared, script(), None, None)
            .await
            .expect("assemble");

        let (event, rx) = Event::fetch(get_request("https://example.com/"));

        worker.exec(event).await.expect("exec");

        let response = rx.await.expect("response");

        assert_eq!(response.status, 200);
    }
}

#[tokio::test]
async fn a_prepared_component_refuses_the_other_fuel_mode() {
    let prepared =
        WasmWorker::prepare(&load_component("hello-worker"), None).expect("prepare should compile");

    // The default limits carry a CPU budget, so the prepared component is
    // metered; a worker without one must be refused.
    let unmetered = RuntimeLimits {
        max_cpu_time_ms: 0,
        ..Default::default()
    };

    let error = match WasmWorker::from_prepared(&prepared, script(), Some(unmetered), None).await {
        Ok(_) => panic!("fuel modes disagree, assembly should refuse"),
        Err(error) => error,
    };

    assert!(
        matches!(&error, TerminationReason::InitializationError(msg) if msg.contains("fuel")),
        "unexpected error: {error:?}"
    );
}
