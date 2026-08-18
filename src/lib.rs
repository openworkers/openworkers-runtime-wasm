//! WebAssembly runtime for OpenWorkers using Wasmtime
//!
//! Executes WebAssembly components (compiled from Rust, Go, C++, ...) behind
//! the same `openworkers_core::Worker` interface as the JS runtimes.
//!
//! Guests target the `openworkers:worker` WIT world (see `wit/worker.wit`):
//! they export the `handler` interface (`handle-fetch`, `handle-scheduled`)
//! and can import the `host` interface (`log`, `get-env`, `fetch`). Host
//! operations are delegated to the runner through `OperationsHandler`.

mod worker;

pub use worker::WasmWorker;
