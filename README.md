# openworkers-runtime-wasm

WebAssembly runtime for OpenWorkers using Wasmtime Component Model.

## Features

- **Component Model**: Type-safe host/guest communication via WIT interfaces
- **WASI Support**: WebAssembly System Interface (WASIp2)
- **Multi-language**: Write workers in Rust, Go (TinyGo), C/C++, AssemblyScript
- **Secure by design**: Capabilities-based sandboxing, no Spectre concerns

## Architecture

```
┌─────────────────────────────────────────────┐
│            openworkers-runner               │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│          openworkers-runtime-wasm           │
│  ┌───────────────────────────────────────┐  │
│  │            Wasmtime                   │  │
│  │  ┌─────────────────────────────────┐  │  │
│  │  │     WASM Component (Worker)     │  │  │
│  │  │  - handle_fetch()               │  │  │
│  │  │  - handle_scheduled()           │  │  │
│  │  └─────────────────────────────────┘  │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

## WIT Interface

Workers implement the `handler` interface defined in `wit/worker.wit`:

```wit
interface handler {
    handle-fetch: func(request: http-request) -> http-response;
    handle-scheduled: func(scheduled-time: u64);
}
```

Host provides:
- `log(level, message)` - Logging
- `get-env(key)` - Environment variables

## Writing a Worker (Rust)

```rust
wit_bindgen::generate!({
    world: "worker",
    path: "wit/worker.wit",
});

use exports::openworkers::worker::handler::Guest;
use openworkers::worker::types::{HttpRequest, HttpResponse};

struct MyWorker;

impl Guest for MyWorker {
    fn handle_fetch(request: HttpRequest) -> HttpResponse {
        HttpResponse {
            status: 200,
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: Some(b"Hello from WASM!".to_vec()),
        }
    }

    fn handle_scheduled(scheduled_time: u64) {
        // Handle cron job
    }
}

export!(MyWorker);
```

Build:
```bash
cargo build --target wasm32-wasip2 --release
```

## Usage

```rust
use openworkers_runtime_wasm::WasmWorker;
use openworkers_core::{Script, WorkerCode, Task};

// Load WASM component
let wasm_bytes = std::fs::read("worker.wasm")?;
let script = Script::new(WorkerCode::WebAssembly(wasm_bytes));

// Create worker
let mut worker = WasmWorker::new(script, None).await?;

// Execute task
worker.exec(task).await?;
```

## Examples

See `examples/hello-worker` for a complete example.

```bash
# Build the example
cd examples/hello-worker
cargo build --target wasm32-wasip2 --release

# Run tests
cd ../..
cargo test
```

## License

MIT
