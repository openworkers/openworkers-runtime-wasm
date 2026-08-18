//! WebAssembly runtime for OpenWorkers using Wasmtime
//!
//! Executes WebAssembly components (compiled from Rust, Go, C++, ...) behind
//! the same `openworkers_core::Worker` interface as the JS runtimes.
//!
//! HTTP guests are plain `wasi:http/proxy` components: they export
//! `wasi:http/incoming-handler` and their `wasi:http/outgoing-handler` imports
//! are served by the runner's `OperationsHandler`. Cron guests additionally
//! export `openworkers:worker/scheduled` (see `wit/worker.wit`).

mod worker;

pub use worker::WasmWorker;
