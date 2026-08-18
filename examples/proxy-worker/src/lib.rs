//! Stock `wasi:http/proxy` component: it knows nothing about OpenWorkers and
//! is built from the upstream `wasi` crate alone.
//!
//! Build with: cargo build --target wasm32-wasip2 --release

use wasi::exports::http::incoming_handler::Guest;
use wasi::http::types::Fields;
use wasi::http::types::IncomingRequest;
use wasi::http::types::OutgoingBody;
use wasi::http::types::OutgoingResponse;
use wasi::http::types::ResponseOutparam;

struct ProxyWorker;

impl Guest for ProxyWorker {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let body = format!(
            "stock wasi:http proxy answering {}",
            request.path_with_query().unwrap_or_default()
        );

        let response = OutgoingResponse::new(Fields::new());
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

wasi::http::proxy::export!(ProxyWorker);
