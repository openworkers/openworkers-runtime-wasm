//! Hello World worker for OpenWorkers WASM runtime (Component Model)
//!
//! Build with: cargo build --target wasm32-wasip2 --release
//! Output: target/wasm32-wasip2/release/hello_worker.wasm

wit_bindgen::generate!({
    world: "worker",
    path: "../../wit",
    generate_all,
});

use exports::openworkers::worker::scheduled::Guest as ScheduledGuest;
use exports::wasi::http::incoming_handler::Guest as HttpGuest;
use wasi::http::outgoing_handler;
use wasi::http::types::Fields;
use wasi::http::types::IncomingBody;
use wasi::http::types::IncomingRequest;
use wasi::http::types::Method;
use wasi::http::types::OutgoingBody;
use wasi::http::types::OutgoingRequest;
use wasi::http::types::OutgoingResponse;
use wasi::http::types::ResponseOutparam;
use wasi::http::types::Scheme;

/// wasi:io caps `blocking-write-and-flush` at this many bytes
const CHUNK_SIZE: usize = 4096;

struct HelloWorker;

impl HttpGuest for HelloWorker {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let authority = request.authority().unwrap_or_default();

        println!(
            "Received {:?} request to {}{}",
            request.method(),
            authority,
            path
        );

        if path.starts_with("/proxy") {
            let (status, body) = proxy_upstream();

            respond(response_out, status, body);
            return;
        }

        if path.starts_with("/echo") {
            let body = drain(request.consume().unwrap());

            respond(response_out, 200, body);
            return;
        }

        if path.starts_with("/spin") {
            loop {
                std::hint::black_box(0);
            }
        }

        if path.starts_with("/alloc") {
            let mut hog: Vec<Vec<u8>> = Vec::new();

            loop {
                hog.push(vec![0u8; 1 << 20]);
                std::hint::black_box(&hog);
            }
        }

        let greeting = std::env::var("GREETING").unwrap_or_else(|_| "Hello".to_string());

        let body = format!(
            "{} from Rust WASM!\nYou requested: {}{}",
            greeting, authority, path
        );

        respond(response_out, 200, body.into_bytes());
    }
}

impl ScheduledGuest for HelloWorker {
    fn handle_scheduled(scheduled_time: u64) {
        println!("Scheduled event at timestamp: {}", scheduled_time);
    }
}

/// Fetch a fixed upstream URL through wasi:http/outgoing-handler and relay it
fn proxy_upstream() -> (u16, Vec<u8>) {
    let request = OutgoingRequest::new(Fields::new());
    request.set_method(&Method::Get).unwrap();
    request.set_scheme(Some(&Scheme::Https)).unwrap();
    request.set_authority(Some("upstream.example")).unwrap();
    request.set_path_with_query(Some("/data")).unwrap();

    let request_body = request.body().unwrap();
    let pending = outgoing_handler::handle(request, None);

    OutgoingBody::finish(request_body, None).unwrap();

    let pending = match pending {
        Ok(pending) => pending,
        Err(e) => return (502, format!("upstream fetch failed: {}", e).into_bytes()),
    };

    pending.subscribe().block();

    let response = pending
        .get()
        .expect("subscribe returned before the response was ready")
        .expect("response taken twice");

    match response {
        Ok(response) => (response.status(), drain(response.consume().unwrap())),
        Err(e) => (502, format!("upstream fetch failed: {}", e).into_bytes()),
    }
}

fn respond(response_out: ResponseOutparam, status: u16, body: Vec<u8>) {
    let headers = Fields::new();

    headers
        .set("content-type", &[b"text/plain".to_vec()])
        .unwrap();

    let response = OutgoingResponse::new(headers);
    response.set_status_code(status).unwrap();

    let response_body = response.body().unwrap();

    ResponseOutparam::set(response_out, Ok(response));

    {
        let stream = response_body.write().unwrap();

        for chunk in body.chunks(CHUNK_SIZE) {
            stream.blocking_write_and_flush(chunk).unwrap();
        }
    }

    OutgoingBody::finish(response_body, None).unwrap();
}

/// Read a body to its end; `blocking-read` reports the end as `Err(closed)`
fn drain(body: IncomingBody) -> Vec<u8> {
    let stream = body.stream().unwrap();
    let mut bytes = Vec::new();

    while let Ok(chunk) = stream.blocking_read(CHUNK_SIZE as u64) {
        bytes.extend_from_slice(&chunk);
    }

    bytes
}

export!(HelloWorker);
