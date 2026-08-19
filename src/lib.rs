//! WebAssembly runtime for OpenWorkers using Wasmtime
//!
//! Executes WebAssembly components (compiled from Rust, Go, C++, ...) behind
//! the same `openworkers_core::Worker` interface as the JS runtimes.
//!
//! HTTP guests are plain `wasi:http/proxy` components: they export
//! `wasi:http/incoming-handler` and their `wasi:http/outgoing-handler` imports
//! are served by the runner's `OperationsHandler`. Cron guests additionally
//! export `openworkers:worker/scheduled` (see `wit/worker.wit`), and guests
//! that need a database, KV or object storage import `openworkers:bindings`
//! (see `wit/bindings.wit`).

mod bindings;
mod worker;

pub use worker::WasmWorker;
