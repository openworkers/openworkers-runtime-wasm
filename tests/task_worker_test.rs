//! The task export: a guest that has it gets every task with its payload and
//! source, and the host calls it in place of `scheduled`.

use openworkers_core::{Event, Script, TaskResult, TaskSource, WorkerCode};
use openworkers_runtime_wasm::WasmWorker;
use serde_json::json;

/// Built by `cd examples/task-worker && cargo build --target wasm32-wasip2 --release`
fn task_worker() -> Script {
    let path = format!(
        "{}/examples/task-worker/target/wasm32-wasip2/release/task_worker.wasm",
        env!("CARGO_MANIFEST_DIR")
    );
    let bytes = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "could not read {path}: {e}\n\
             build it with: cd examples/task-worker && cargo build --target wasm32-wasip2 --release"
        )
    });

    Script {
        code: WorkerCode::WebAssembly(bytes),
        env: None,
        bindings: vec![],
    }
}

async fn run(event: Event, rx: tokio::sync::oneshot::Receiver<TaskResult>) -> TaskResult {
    let mut worker = WasmWorker::new(task_worker(), None, None).await.unwrap();
    worker.exec(event).await.unwrap();

    rx.await.unwrap()
}

#[tokio::test]
async fn a_cron_task_reaches_the_task_export_with_its_cron() {
    let source = TaskSource::Schedule {
        time: 1_700_000_000_000,
        cron: Some("*/5 * * * *".to_string()),
    };
    let (event, rx) = Event::task("cron-1".to_string(), None, Some(source), 1);
    let result = run(event, rx).await;

    assert!(result.success, "{:?}", result.error);
    assert_eq!(
        result.data,
        Some(json!({
            "taskId": "cron-1",
            "attempt": 1,
            "payload": null,
            "source": { "type": "schedule", "time": 1_700_000_000_000u64, "cron": "*/5 * * * *" },
        }))
    );
}

#[tokio::test]
async fn an_invoked_task_carries_its_payload() {
    let (event, rx) = Event::invoke(
        "invoke-1".to_string(),
        Some(json!({ "n": 3 })),
        Some("cli".to_string()),
    );
    let result = run(event, rx).await;

    assert!(result.success, "{:?}", result.error);
    let data = result.data.unwrap();
    assert_eq!(data["payload"], json!({ "n": 3 }));
    assert_eq!(data["source"], json!({ "type": "invoke", "origin": "cli" }));
}

#[tokio::test]
async fn a_chained_task_names_its_parent() {
    let source = TaskSource::Chained {
        parent_task_id: "parent-1".to_string(),
        parent_worker_id: "worker-1".to_string(),
        parent_worker_name: None,
    };
    let (event, rx) = Event::task("child-1".to_string(), None, Some(source), 2);
    let result = run(event, rx).await;

    let data = result.data.unwrap();
    assert_eq!(data["attempt"], json!(2));
    assert_eq!(
        data["source"],
        json!({ "type": "chained", "parent": "parent-1" })
    );
}

#[tokio::test]
async fn an_error_from_the_guest_fails_the_task() {
    let (event, rx) = Event::invoke("invoke-2".to_string(), Some(json!("fail")), None);
    let result = run(event, rx).await;

    assert!(!result.success);
    assert_eq!(result.error.as_deref(), Some("asked to fail"));
}
