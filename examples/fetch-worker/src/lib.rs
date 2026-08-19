//! Worker targeting `world fetch-worker`: the proxy and the platform bindings
//! without the cron export.
//!
//! Build with: cargo build --target wasm32-wasip2 --release
//! Output: target/wasm32-wasip2/release/fetch_worker.wasm

wit_bindgen::generate!({
    world: "fetch-worker",
    path: "../../wit",
    generate_all,
});

use exports::wasi::http::incoming_handler::Guest as HttpGuest;
use openworkers::bindings::kv;
use wasi::http::types::Fields;
use wasi::http::types::IncomingRequest;
use wasi::http::types::OutgoingBody;
use wasi::http::types::OutgoingResponse;
use wasi::http::types::ResponseOutparam;

struct FetchWorker;

impl HttpGuest for FetchWorker {
    fn handle(_request: IncomingRequest, response_out: ResponseOutparam) {
        let body = match kv_probe() {
            Ok(value) => value,
            Err(e) => e,
        };

        let headers = Fields::new();

        headers
            .set("content-type", &[b"text/plain".to_vec()])
            .unwrap();

        let response = OutgoingResponse::new(headers);
        response.set_status_code(200).unwrap();

        let response_body = response.body().unwrap();

        ResponseOutparam::set(response_out, Ok(response));

        {
            let stream = response_body.write().unwrap();
            stream.blocking_write_and_flush(body.as_bytes()).unwrap();
        }

        OutgoingBody::finish(response_body, None).unwrap();
    }
}

/// Reaches a binding, so the response proves the world's imports are linked
fn kv_probe() -> Result<String, String> {
    kv::put("CACHE", "greeting", "\"hello\"", None)?;

    Ok(kv::get("CACHE", "greeting")?.unwrap_or_else(|| "absent".to_string()))
}

export!(FetchWorker);
