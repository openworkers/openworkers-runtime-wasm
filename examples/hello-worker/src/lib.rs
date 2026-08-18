//! Hello World worker for OpenWorkers WASM runtime (Component Model)
//!
//! Build with: cargo build --target wasm32-wasip2 --release
//! Output: target/wasm32-wasip2/release/hello_worker.wasm

// Generate bindings from the WIT file
wit_bindgen::generate!({
    world: "worker",
    path: "wit/worker.wit",
});

// Import the generated types
use exports::openworkers::worker::handler::Guest;
use openworkers::worker::host;
use openworkers::worker::types::{HttpMethod, HttpRequest, HttpResponse};

struct HelloWorker;

impl Guest for HelloWorker {
    fn handle_fetch(request: HttpRequest) -> HttpResponse {
        // Log the request
        host::log(
            1,
            &format!("Received {:?} request to {}", request.method, request.url),
        );

        if request.url.contains("/proxy") {
            return proxy_upstream();
        }

        // Limit-testing endpoints
        if request.url.contains("/spin") {
            loop {
                std::hint::black_box(0);
            }
        }

        if request.url.contains("/alloc") {
            let mut hog: Vec<Vec<u8>> = Vec::new();

            loop {
                hog.push(vec![0u8; 1 << 20]);
                std::hint::black_box(&hog);
            }
        }

        // Get greeting from env or use default
        let greeting = host::get_env("GREETING").unwrap_or_else(|| "Hello".to_string());

        // Build response headers
        let headers = vec![
            ("Content-Type".to_string(), "text/plain".to_string()),
            ("X-Powered-By".to_string(), "OpenWorkers-WASM".to_string()),
        ];

        // Build response body
        let body = format!(
            "{} from Rust WASM!\nYou requested: {}",
            greeting, request.url
        );

        HttpResponse {
            status: 200,
            headers,
            body: Some(body.into_bytes()),
        }
    }

    fn handle_scheduled(scheduled_time: u64) {
        host::log(
            1,
            &format!("Scheduled event at timestamp: {}", scheduled_time),
        );
    }
}

/// Fetch a fixed upstream URL through host.fetch and relay the result
fn proxy_upstream() -> HttpResponse {
    let upstream = HttpRequest {
        method: HttpMethod::Get,
        url: "https://upstream.example/data".to_string(),
        headers: vec![],
        body: None,
    };

    let headers = vec![("Content-Type".to_string(), "text/plain".to_string())];

    match host::fetch(&upstream) {
        Ok(response) => HttpResponse {
            status: response.status,
            headers,
            body: response.body,
        },
        Err(e) => HttpResponse {
            status: 502,
            headers,
            body: Some(format!("upstream fetch failed: {}", e).into_bytes()),
        },
    }
}

export!(HelloWorker);
