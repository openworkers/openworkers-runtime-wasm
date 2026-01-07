//! WebAssembly runtime for OpenWorkers using Wasmtime
//!
//! This runtime executes WebAssembly modules compiled from Rust, Go, C++, etc.
//! It provides the same Worker interface as the V8 runtime but for WASM workloads.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    OpenWorkers Runner                    │
//! ├─────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐       ┌─────────────────────────┐  │
//! │  │  runtime-v8     │       │   runtime-wasm          │  │
//! │  │  (JavaScript)   │       │   (Rust/Go/C++ → WASM)  │  │
//! │  └─────────────────┘       └─────────────────────────┘  │
//! │           │                           │                  │
//! │           └───────────┬───────────────┘                  │
//! │                       ▼                                  │
//! │              openworkers-core                            │
//! │         (Worker trait, Task, HttpRequest, etc.)          │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Guest Interface (what WASM modules export)
//!
//! WASM modules must export these functions:
//! - `handle_fetch(request_ptr, request_len) -> response_ptr`
//! - `handle_scheduled(time: i64) -> i32`
//!
//! And import these host functions:
//! - `host_log(level, msg_ptr, msg_len)`
//! - `host_fetch(request_ptr, request_len) -> response_ptr`
//! - `host_kv_get(key_ptr, key_len) -> value_ptr`
//! - etc.

mod worker;

pub use worker::WasmWorker;
