# openworkers-runtime-wasm

WebAssembly runtime for OpenWorkers using Wasmtime Component Model.

## Features

- **Standard HTTP contract**: guests are `wasi:http/proxy` components
- **WASI Support**: WebAssembly System Interface (WASIp2)
- **Multi-language**: Write workers in Rust, Go (TinyGo), C/C++, AssemblyScript
- **Secure by design**: Capabilities-based sandboxing, no Spectre concerns

## Architecture

```
+---------------------------------------------+
|            openworkers-runner               |
+----------------------+----------------------+
                       |
+----------------------v----------------------+
|          openworkers-runtime-wasm           |
|  +---------------------------------------+  |
|  |            Wasmtime                   |  |
|  |  +---------------------------------+  |  |
|  |  |     WASM Component (Worker)     |  |  |
|  |  |  - wasi:http/incoming-handler   |  |  |
|  |  |  - openworkers:worker/scheduled |  |  |
|  |  +---------------------------------+  |  |
|  +---------------------------------------+  |
+---------------------------------------------+
```

## Interfaces

A worker is a WASI Preview 2 component that exports
`wasi:http/incoming-handler`, `openworkers:worker/scheduled`, or both. Any
component built against the standard `wasi:http/proxy` world runs unmodified;
`wit/worker.wit` adds the cron entry point on top of it:

```wit
world fetch-worker {
    include wasi:http/proxy@0.2.0;
    include openworkers:bindings/imports@0.1.0;
    import wasi:cli/environment@0.2.0;
}

world worker {
    include fetch-worker;
    include scheduled-only;
}
```

Target `fetch-worker` when the worker only serves HTTP and `worker` when it also
handles cron; the two differ by the `scheduled` export alone, which is how the
host tells them apart. `scheduled-only` carries that export on its own, for an
SDK that adds it to `fetch-worker` only when the guest has a handler.

`task-only` carries the `task` export, which gets every task the platform runs:
cron, chained, sent from a worker or invoked. Its event holds the task id, the
attempt, the payload as JSON text and the source, with the cron expression for
a cron task; the handler answers JSON text or an error message. When a guest
exports `task`, the host calls it for every task and never calls `scheduled`.
A guest with `scheduled` alone gets the trigger time and nothing else.

wasi is declared at `0.2.0`, the floor of the 0.2 line, not at the host's patch
level: every 0.2.x is semver-compatible and both wasm-tools and wasmtime resolve
a 0.2.0 import or export against the newer implementation, so a toolchain that
lags still builds against this contract.

The host provides:

- `wasi:http/outgoing-handler` - outbound requests, routed to the runner's
  `OperationsHandler`
- `wasi:cli/environment` - the worker's environment variables
- `wasi:cli/stdout` and `wasi:cli/stderr` - guest output, forwarded to the
  runner's log handler line by line (stdout as info, stderr as error)
- `openworkers:bindings/{database,kv,storage}` - the platform bindings, see
  `wit/bindings.wit`

Bodies are buffered at the boundary in both directions; streaming pass-through
is not implemented yet.

### Bindings

Every binding call names its binding first, because a worker can hold several
bindings of the same type:

```rust
use openworkers::bindings::database::{self, SqlParam, SqlValue};

let params = [SqlParam::Value(SqlValue::Integer(42))];
let row = database::first("DB", "SELECT * FROM items WHERE id = $1", &params)?;
```

`database` is shaped after D1: preparing a statement and binding its
parameters happens guest-side, and rows come back as JSON text. `kv` values
are JSON documents; `storage` bodies are opaque bytes.

## Limits

| `RuntimeLimits` field | Mechanism |
|---|---|
| `max_cpu_time_ms` | fuel, 0 disables metering |
| `max_wall_clock_time_ms` | epoch interruption, 0 disables the deadline |
| `heap_max_mb` | a store memory limiter, 0 disables the cap |

`heap_initial_mb` has no wasm counterpart: a component's memory starts at the
size its module declares.

Fuel is charged per wasm operation, so what a millisecond of CPU buys is a
property of the machine: the runtime measures it once per process by burning a
known loop. `OW_WASM_FUEL_PER_MS` pins the rate instead, for a host that would
rather not measure or wants every node to charge the same.

## Writing a Worker (Rust)

```rust
wit_bindgen::generate!({
    world: "worker",
    path: "wit",
    generate_all,
});

use exports::openworkers::worker::scheduled::Guest as ScheduledGuest;
use exports::wasi::http::incoming_handler::Guest as HttpGuest;
use wasi::http::types::{Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};

struct MyWorker;

impl HttpGuest for MyWorker {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let response = OutgoingResponse::new(Fields::new());
        let body = response.body().unwrap();

        ResponseOutparam::set(response_out, Ok(response));

        {
            let stream = body.write().unwrap();
            stream.blocking_write_and_flush(b"Hello from WASM!").unwrap();
        }

        OutgoingBody::finish(body, None).unwrap();
    }
}

impl ScheduledGuest for MyWorker {
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
let mut worker = WasmWorker::new(script, None, None).await?;

// Execute task
worker.exec(task).await?;
```

## Examples

- `examples/hello-worker` - HTTP plus cron, outbound fetch, environment
- `examples/fetch-worker` - `world fetch-worker`: HTTP and the bindings, no cron
- `examples/proxy-worker` - a stock `wasi:http/proxy` component, built from the
  upstream `wasi` crate with no OpenWorkers-specific WIT
- `examples/task-worker` - the `task` export next to `scheduled`: echoes each task
  as JSON and shows the host calls `task`

```bash
# Build the examples
(cd examples/hello-worker && cargo build --target wasm32-wasip2 --release)
(cd examples/fetch-worker && cargo build --target wasm32-wasip2 --release)
(cd examples/proxy-worker && cargo build --target wasm32-wasip2 --release)
(cd examples/task-worker && cargo build --target wasm32-wasip2 --release)

# Run tests
cargo test
```

## License

MIT
