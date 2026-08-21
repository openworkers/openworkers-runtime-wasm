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
//!
//! Compiling a component is the bulk of a cold start, so a host that runs the
//! same worker again can hold on to the machine code: see [`precompile`] and
//! [`WasmWorker::new_precompiled`].

mod bindings;
mod fuel;
mod precompile;
mod worker;

pub use precompile::PrecompiledComponent;
pub use precompile::compatibility_key;
pub use precompile::precompile;
pub use worker::WasmWorker;
