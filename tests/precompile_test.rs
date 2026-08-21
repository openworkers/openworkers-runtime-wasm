//! Precompilation: the artifact serves the same responses as a freshly
//! compiled component, and only the explicit path accepts one.

use openworkers_core::{
    Event, HttpMethod, HttpRequest, HttpResponse, RequestBody, ResponseBody, RuntimeLimits, Script,
    TerminationReason, WorkerCode,
};
use openworkers_runtime_wasm::{PrecompiledComponent, WasmWorker, compatibility_key, precompile};
use std::collections::HashMap;

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

fn hello_script(wasm_bytes: Vec<u8>) -> Script {
    Script {
        code: WorkerCode::WebAssembly(wasm_bytes),
        env: Some(HashMap::from([(
            "GREETING".to_string(),
            "Bonjour".to_string(),
        )])),
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

async fn serve(worker: &mut WasmWorker, url: &str) -> HttpResponse {
    let (event, rx) = Event::fetch(get_request(url));

    worker.exec(event).await.expect("Failed to execute event");

    rx.await.expect("Failed to receive response")
}

/// `WasmWorker` is not `Debug`, so an unexpected success has to report itself
fn init_error(result: Result<WasmWorker, TerminationReason>, what: &str) -> String {
    match result {
        Ok(_) => panic!("{what} should have been refused"),
        Err(TerminationReason::InitializationError(message)) => message,
        Err(other) => panic!("expected an initialization error for {what}, got {other:?}"),
    }
}

fn body_of(response: &HttpResponse) -> String {
    match &response.body {
        ResponseBody::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("expected a buffered body, got {other:?}"),
    }
}

#[tokio::test]
async fn a_precompiled_component_serves_the_same_response() {
    let wasm_bytes = load_component("hello-worker");

    let artifact = precompile(&wasm_bytes, None).expect("hello-worker should precompile");

    assert!(
        !artifact.starts_with(b"\0asm"),
        "the artifact is machine code, not wasm"
    );

    let mut compiled = WasmWorker::new(hello_script(wasm_bytes), None, None)
        .await
        .expect("Failed to create worker");

    let expected = serve(&mut compiled, "https://example.com/test").await;

    // SAFETY: the artifact comes from `precompile` a few lines above
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(artifact) };

    let mut loaded = WasmWorker::new_precompiled(component, hello_script(Vec::new()), None, None)
        .await
        .expect("the artifact should load");

    let actual = serve(&mut loaded, "https://example.com/test").await;

    assert_eq!(actual.status, expected.status);
    assert_eq!(actual.headers, expected.headers);
    assert_eq!(body_of(&actual), body_of(&expected));
    assert!(
        body_of(&actual).contains("Bonjour"),
        "env should reach the guest"
    );
}

/// A host that just compiled a guest keeps the machine code from the worker
/// itself, rather than paying Cranelift a second time
#[tokio::test]
async fn a_worker_serializes_the_component_it_compiled() {
    let wasm_bytes = load_component("hello-worker");

    let mut compiled = WasmWorker::new(hello_script(wasm_bytes), None, None)
        .await
        .expect("Failed to create worker");

    let expected = serve(&mut compiled, "https://example.com/test").await;

    let artifact = compiled
        .serialize_component()
        .expect("a compiled component should serialize");

    // SAFETY: the artifact comes from the worker built a few lines above
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(artifact) };

    let mut loaded = WasmWorker::new_precompiled(component, hello_script(Vec::new()), None, None)
        .await
        .expect("the artifact should load");

    let actual = serve(&mut loaded, "https://example.com/test").await;

    assert_eq!(actual.status, expected.status);
    assert_eq!(body_of(&actual), body_of(&expected));
}

/// The scheduled export survives the round trip too, so a cron worker is not
/// silently downgraded to an HTTP-only one
#[tokio::test]
async fn a_precompiled_component_keeps_its_scheduled_export() {
    let wasm_bytes = load_component("hello-worker");

    let artifact = precompile(&wasm_bytes, None).expect("hello-worker should precompile");

    // SAFETY: the artifact comes from `precompile` a few lines above
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(artifact) };

    let mut worker = WasmWorker::new_precompiled(component, hello_script(Vec::new()), None, None)
        .await
        .expect("the artifact should load");

    let (event, rx) = Event::from_schedule("precompiled-task".to_string(), 1_700_000_000);

    worker.exec(event).await.expect("scheduled should run");

    assert!(rx.await.expect("worker should answer").success);
}

/// The whole point of the separate constructor: an artifact-shaped upload has
/// to be compiled as wasm, which it is not, rather than deserialized
#[tokio::test]
async fn the_compiling_path_refuses_an_artifact() {
    let artifact =
        precompile(&load_component("hello-worker"), None).expect("hello-worker should precompile");

    let message = init_error(
        WasmWorker::new(hello_script(artifact), None, None).await,
        "an artifact submitted as worker code",
    );

    assert!(message.contains("\\0asm"), "unexpected error: {message}");
}

#[tokio::test]
async fn the_compiling_path_refuses_elf_bytes() {
    let elf = b"\x7fELF\x02\x01\x01\x00 and whatever follows".to_vec();

    let message = init_error(
        WasmWorker::new(hello_script(elf), None, None).await,
        "ELF bytes submitted as worker code",
    );

    assert!(message.contains("\\0asm"), "unexpected error: {message}");
}

/// A cache can hand back a half-written entry; wasmtime has to reject it
/// instead of mapping it in
#[tokio::test]
async fn a_truncated_artifact_fails_to_load() {
    let mut artifact =
        precompile(&load_component("hello-worker"), None).expect("hello-worker should precompile");

    artifact.truncate(artifact.len() / 2);

    // SAFETY: still `precompile` output, only cut short; the point of the test
    // is that wasmtime notices before running any of it
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(artifact) };

    let message = init_error(
        WasmWorker::new_precompiled(component, hello_script(Vec::new()), None, None).await,
        "a truncated artifact",
    );

    assert!(
        message.contains("precompiled"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn an_empty_artifact_fails_to_load() {
    // SAFETY: no bytes to be wrong about
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(Vec::new()) };

    let message = init_error(
        WasmWorker::new_precompiled(component, hello_script(Vec::new()), None, None).await,
        "an empty artifact",
    );

    assert!(
        message.contains("precompiled"),
        "unexpected error: {message}"
    );
}

/// Fuel metering changes the emitted code, so the artifact and the worker have
/// to agree on it; wasmtime reports the mismatch rather than running the wrong
/// code
#[tokio::test]
async fn an_artifact_built_under_other_limits_fails_to_load() {
    let metered = RuntimeLimits {
        max_cpu_time_ms: 100,
        ..Default::default()
    };

    let unmetered = RuntimeLimits {
        max_cpu_time_ms: 0,
        ..Default::default()
    };

    let artifact = precompile(&load_component("hello-worker"), Some(metered.clone()))
        .expect("hello-worker should precompile");

    // SAFETY: the artifact comes from `precompile` a few lines above
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(artifact) };

    let message = init_error(
        WasmWorker::new_precompiled(
            component,
            hello_script(Vec::new()),
            Some(unmetered.clone()),
            None,
        )
        .await,
        "an artifact built under other limits",
    );

    assert!(
        message.contains("precompiled"),
        "unexpected error: {message}"
    );

    assert_ne!(
        compatibility_key(Some(metered)).expect("engine should build"),
        compatibility_key(Some(unmetered)).expect("engine should build"),
        "a cache keyed on this would have to notice"
    );
}

/// Prints what precompilation buys; run with
/// `cargo test --test precompile_test -- --ignored --nocapture`
#[tokio::test]
#[ignore = "measurement, not an assertion"]
async fn report_cold_start_cost() {
    let wasm_bytes = load_component("hello-worker");

    let start = std::time::Instant::now();
    let artifact = precompile(&wasm_bytes, None).expect("hello-worker should precompile");
    let precompile_time = start.elapsed();

    let start = std::time::Instant::now();
    let mut compiled = WasmWorker::new(hello_script(wasm_bytes.clone()), None, None)
        .await
        .expect("Failed to create worker");
    let compile_time = start.elapsed();

    let start = std::time::Instant::now();
    serve(&mut compiled, "https://example.com/test").await;
    let compiled_first_exec = start.elapsed();

    let start = std::time::Instant::now();
    compiled
        .serialize_component()
        .expect("a compiled component should serialize");
    let serialize_time = start.elapsed();

    let start = std::time::Instant::now();
    // SAFETY: the artifact comes from `precompile` above
    let component = unsafe { PrecompiledComponent::from_trusted_bytes(artifact.clone()) };
    let mut loaded = WasmWorker::new_precompiled(component, hello_script(Vec::new()), None, None)
        .await
        .expect("the artifact should load");
    let deserialize_time = start.elapsed();

    let start = std::time::Instant::now();
    serve(&mut loaded, "https://example.com/test").await;
    let loaded_first_exec = start.elapsed();

    let start = std::time::Instant::now();
    compatibility_key(None).expect("engine should build");
    let key_time = start.elapsed();

    println!(
        "wasm {} bytes, artifact {} bytes",
        wasm_bytes.len(),
        artifact.len()
    );
    println!("  compile (WasmWorker::new):       {compile_time:?}");
    println!("  precompile (compile + serialize):{precompile_time:?}");
    println!("  serialize an existing component: {serialize_time:?}");
    println!("  load artifact (new_precompiled): {deserialize_time:?}");
    println!("  compatibility_key:               {key_time:?}");
    println!("  first exec, compiled:            {compiled_first_exec:?}");
    println!("  first exec, precompiled:         {loaded_first_exec:?}");
}
