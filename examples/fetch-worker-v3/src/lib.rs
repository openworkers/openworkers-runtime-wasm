//! A guest for the WASI 0.3 `fetch-worker-v3` world, exercising what the 0.2
//! world cannot express: post-response work and bodies that never sit whole
//! in guest memory.
//!
//! Build with: cargo build --target wasm32-wasip2 --release
//! Output: target/wasm32-wasip2/release/fetch_worker_v3.wasm

wit_bindgen::generate!({
    world: "fetch-worker-v3",
    path: "../../wit",
    generate_all,
});

use exports::wasi::http0_3_0::handler::Guest;
use openworkers::bindings::kv;
use wasi::http0_3_0::client;
use wasi::http0_3_0::types::ErrorCode;
use wasi::http0_3_0::types::Fields;
use wasi::http0_3_0::types::Request;
use wasi::http0_3_0::types::Response;
use wit_bindgen::rt::async_support::StreamResult;

/// Bodies flow through in pieces of this size, so peak guest memory stays a
/// chunk, not a body
const CHUNK_SIZE: usize = 64 * 1024;

struct FetchWorkerV3;

impl Guest for FetchWorkerV3 {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();

        match path.split('?').next().unwrap_or_default() {
            "/consume" => consume(request).await,
            "/generate" => generate(&path),
            "/wait-until" => wait_until(),
            "/fetch" => outbound().await,
            _ => respond("hello from v3"),
        }
    }
}

/// A response whose whole body is already in memory; the stream and trailers
/// still have to be fed from a task, because the host only starts reading
/// after `handle` returns.
fn respond(text: &str) -> Result<Response, ErrorCode> {
    let bytes = text.as_bytes().to_vec();

    let (mut body_tx, body_rx) = wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));
    let (response, _transmit) = Response::new(Fields::new(), Some(body_rx), trailers_rx);

    wit_bindgen::spawn_local(async move {
        let _ = body_tx.write_all(bytes).await;
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });

    Ok(response)
}

/// Reads the request body chunk by chunk and answers with the byte count;
/// the body never exists whole on this side of the boundary.
async fn consume(request: Request) -> Result<Response, ErrorCode> {
    let (done_tx, done_rx) = wit_future::new(|| Ok(()));
    let (mut stream, _trailers) = Request::consume_body(request, done_rx);

    let mut total: u64 = 0;

    loop {
        let (result, buf) = stream.read(Vec::with_capacity(CHUNK_SIZE)).await;

        total += buf.len() as u64;

        if let StreamResult::Dropped = result {
            break;
        }
    }

    drop(done_tx);

    respond(&total.to_string())
}

/// Streams `mb` megabytes out without ever holding more than a chunk
fn generate(path: &str) -> Result<Response, ErrorCode> {
    let mb: usize = path
        .split_once("mb=")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(1);

    let (mut body_tx, body_rx) = wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));
    let (response, _transmit) = Response::new(Fields::new(), Some(body_rx), trailers_rx);

    wit_bindgen::spawn_local(async move {
        let mut remaining = mb * 1024 * 1024;

        while remaining > 0 {
            let size = remaining.min(CHUNK_SIZE);
            let _ = body_tx.write_all(vec![b'x'; size]).await;
            remaining -= size;
        }

        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });

    Ok(response)
}

/// Post-response work: the kv write happens after `handle` has returned and
/// after the body has been handed over; holding the trailers back until it
/// is done is what lets the host wait for it.
fn wait_until() -> Result<Response, ErrorCode> {
    let (mut body_tx, body_rx) = wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));
    let (response, _transmit) = Response::new(Fields::new(), Some(body_rx), trailers_rx);

    wit_bindgen::spawn_local(async move {
        let _ = body_tx.write_all(b"queued".to_vec()).await;
        drop(body_tx);

        let _ = kv::put("CACHE", "after-response", "\"done\"", None);

        let _ = trailers_tx.write(Ok(None)).await;
    });

    Ok(response)
}

/// Outbound fetch through the async 0.3 `wasi:http/client`
async fn outbound() -> Result<Response, ErrorCode> {
    let headers = Fields::new();
    let (contents_tx, contents_rx) = wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));

    drop(contents_tx);
    drop(trailers_tx);

    let (request, _transmit) = Request::new(headers, Some(contents_rx), trailers_rx, None);

    request
        .set_scheme(Some(&wasi::http0_3_0::types::Scheme::Https))
        .map_err(|()| ErrorCode::InternalError(Some("set_scheme".into())))?;
    request
        .set_authority(Some("upstream.example"))
        .map_err(|()| ErrorCode::InternalError(Some("set_authority".into())))?;
    request
        .set_path_with_query(Some("/data"))
        .map_err(|()| ErrorCode::InternalError(Some("set_path_with_query".into())))?;

    let upstream = client::send(request).await?;

    let (done_tx, done_rx) = wit_future::new(|| Ok(()));
    let (stream, _trailers) = Response::consume_body(upstream, done_rx);
    let bytes = stream.collect().await;

    drop(done_tx);

    respond(&format!("upstream said: {}", String::from_utf8_lossy(&bytes)))
}

export!(FetchWorkerV3);
