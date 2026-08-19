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
use openworkers::bindings::database;
use openworkers::bindings::database::SqlParam;
use openworkers::bindings::database::SqlValue;
use openworkers::bindings::kv;
use openworkers::bindings::storage;
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

/// List items the /render page carries
const RENDERED_ITEMS: usize = 20_000;

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

        if path.starts_with("/db") {
            respond_probe(response_out, database_probe());
            return;
        }

        if path.starts_with("/kv") {
            respond_probe(response_out, kv_probe());
            return;
        }

        if path.starts_with("/storage-meta") {
            respond_probe(response_out, storage_meta_probe());
            return;
        }

        if path.starts_with("/storage-bytes") {
            match storage_bytes_probe() {
                Ok(bytes) => respond(response_out, 200, bytes),
                Err(e) => respond(response_out, 500, e.into_bytes()),
            }

            return;
        }

        if path.starts_with("/echo") {
            let body = drain(request.consume().unwrap());

            respond(response_out, 200, body);
            return;
        }

        if path.starts_with("/longlog") {
            println!("{}", "x".repeat(20_000));

            respond(response_out, 200, Vec::new());
            return;
        }

        if path.starts_with("/render") {
            respond(response_out, 200, render_page().into_bytes());
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

/// A page of the size a real worker renders, so the CPU budget is measured
/// against work rather than against a hello string
fn render_page() -> String {
    let mut page = String::from("<ul>");

    for i in 0..RENDERED_ITEMS {
        page.push_str(&format!("<li>item {} of {}</li>", i, RENDERED_ITEMS));
    }

    page.push_str("</ul>");
    page
}

/// Binds one parameter of every sql-value shape, so the host mapping is
/// exercised end to end
fn database_probe() -> Result<String, String> {
    let params = vec![
        SqlParam::Value(SqlValue::Integer(42)),
        SqlParam::Value(SqlValue::Float(1.5)),
        SqlParam::Value(SqlValue::Text("widget".to_string())),
        SqlParam::Value(SqlValue::Null),
        SqlParam::Value(SqlValue::Boolean(true)),
        SqlParam::Values(vec![SqlValue::Integer(1), SqlValue::Integer(2)]),
    ];

    let first = database::first("DB", "SELECT * FROM items WHERE id = $1", &params)?;
    let all = database::all("DB", "SELECT * FROM items", &[])?;
    let run = database::run("DB", "DELETE FROM items", &[])?;

    Ok(format!(
        "first={} all={} affected={} first-rows={}",
        first.unwrap_or_else(|| "none".to_string()),
        all.rows,
        run.rows_affected,
        all.meta.rows_affected
    ))
}

fn kv_probe() -> Result<String, String> {
    kv::put("CACHE", "greeting", "\"hello\"", Some(60))?;

    let value = kv::get("CACHE", "greeting")?.unwrap_or_else(|| "none".to_string());
    let keys = kv::list_keys("CACHE", Some("gre"), None)?;

    kv::delete("CACHE", "greeting")?;

    Ok(format!(
        "value={} keys={} deleted={}",
        value,
        keys.join(","),
        kv::get("CACHE", "greeting")?.is_none()
    ))
}

/// Every byte value, so a body that survives is not being treated as text
fn storage_body() -> Vec<u8> {
    (0..=u8::MAX).collect()
}

fn storage_bytes_probe() -> Result<Vec<u8>, String> {
    storage::put("BUCKET", "blob.bin", &storage_body())?;

    storage::get("BUCKET", "blob.bin")?.ok_or_else(|| "blob.bin is missing".to_string())
}

fn storage_meta_probe() -> Result<String, String> {
    storage::put("BUCKET", "meta.bin", &storage_body())?;

    let info = storage::head("BUCKET", "meta.bin")?;
    let listing = storage::list_keys("BUCKET", Some("meta"), None)?;

    storage::delete("BUCKET", "meta.bin")?;

    Ok(format!(
        "size={} etag={} keys={} truncated={}",
        info.size,
        info.etag.unwrap_or_else(|| "none".to_string()),
        listing.keys.join(","),
        listing.truncated
    ))
}

fn respond_probe(response_out: ResponseOutparam, probe: Result<String, String>) {
    match probe {
        Ok(body) => respond(response_out, 200, body.into_bytes()),
        Err(e) => respond(response_out, 500, e.into_bytes()),
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
