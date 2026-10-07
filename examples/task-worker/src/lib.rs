//! Worker exporting both `task` and `scheduled`, to show which one a host calls.
//! It answers each task with what it received, as JSON text, and fails a task
//! whose payload is `"fail"`.
//!
//! Build with: cargo build --target wasm32-wasip2 --release
//! Output: target/wasm32-wasip2/release/task_worker.wasm

wit_bindgen::generate!({
    inline: "
        package example:task-worker;

        world task-worker {
            include openworkers:worker/task-only@0.2.0;
            include openworkers:worker/scheduled-only@0.2.0;
        }
    ",
    path: "../../wit",
    world: "example:task-worker/task-worker",
    generate_all,
});

use exports::openworkers::worker::scheduled::Guest as ScheduledGuest;
use exports::openworkers::worker::task::Guest as TaskGuest;
use exports::openworkers::worker::task::TaskEvent;
use exports::openworkers::worker::task::TaskSource;

struct TaskWorker;

impl TaskGuest for TaskWorker {
    fn handle_task(event: TaskEvent) -> Result<Option<String>, String> {
        if event.payload.as_deref() == Some("\"fail\"") {
            return Err("asked to fail".to_string());
        }

        let source = match event.source {
            None => "null".to_string(),
            Some(TaskSource::Schedule(schedule)) => format!(
                r#"{{"type":"schedule","time":{},"cron":{}}}"#,
                schedule.time,
                text(schedule.cron.as_deref())
            ),
            Some(TaskSource::Chained(parent)) => {
                format!(r#"{{"type":"chained","parent":{}}}"#, text(Some(&parent)))
            }
            Some(TaskSource::Worker(worker)) => {
                format!(r#"{{"type":"worker","worker":{}}}"#, text(Some(&worker)))
            }
            Some(TaskSource::Invoke(origin)) => {
                format!(r#"{{"type":"invoke","origin":{}}}"#, text(origin.as_deref()))
            }
        };

        Ok(Some(format!(
            r#"{{"taskId":{},"attempt":{},"payload":{},"source":{}}}"#,
            text(Some(&event.task_id)),
            event.attempt,
            event.payload.as_deref().unwrap_or("null"),
            source
        )))
    }
}

impl ScheduledGuest for TaskWorker {
    fn handle_scheduled(_scheduled_time: u64) {
        panic!("a host that has the task export must not call scheduled");
    }
}

/// A JSON string, or null. The test values hold no character that needs an
/// escape.
fn text(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{value}\""),
        None => "null".to_string(),
    }
}

export!(TaskWorker);
