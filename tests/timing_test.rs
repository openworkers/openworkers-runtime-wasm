//! Latency measurements, run explicitly with --ignored

use openworkers_core::Event;
use openworkers_core::HttpMethod;
use openworkers_core::HttpRequest;
use openworkers_core::RequestBody;
use openworkers_core::Script;
use openworkers_core::WorkerCode;
use openworkers_runtime_wasm::WasmWorker;
use std::collections::HashMap;
use std::time::Instant;

fn load_component(example: &str) -> Vec<u8> {
    let path = format!(
        "{}/examples/{example}/target/wasm32-wasip2/release/{}.wasm",
        env!("CARGO_MANIFEST_DIR"),
        example.replace('-', "_")
    );

    std::fs::read(&path).expect("component not built")
}

#[tokio::test]
#[ignore = "prints timings, run explicitly"]
async fn assemble_and_serve_timing() {
    let prepared =
        WasmWorker::prepare(&load_component("hello-worker"), None).expect("prepare should compile");

    let mut samples = Vec::new();

    for i in 0..50 {
        let script = Script {
            code: WorkerCode::WebAssembly(vec![]),
            env: None,
            bindings: vec![],
        };

        let start = Instant::now();

        let mut worker = WasmWorker::from_prepared(&prepared, script, None, None)
            .await
            .expect("assemble");

        let (event, rx) = Event::fetch(HttpRequest {
            url: "https://example.com/".to_string(),
            method: HttpMethod::Get,
            headers: HashMap::new(),
            body: RequestBody::None,
        });

        worker.exec(event).await.expect("exec");

        let _ = rx.await;

        if i >= 5 {
            samples.push(start.elapsed());
        }
    }

    samples.sort();
    println!(
        "assemble + serve: min {:?}  med {:?}  max {:?}",
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1],
    );
}
