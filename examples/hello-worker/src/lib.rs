//! Hello World worker for OpenWorkers WASM runtime
//!
//! Build with: cargo build --target wasm32-unknown-unknown --release
//! Output: target/wasm32-unknown-unknown/release/hello_worker.wasm

use serde::{Deserialize, Serialize};
use std::alloc::{alloc, Layout};

// ============================================================================
// Host function imports (provided by OpenWorkers runtime)
// ============================================================================

#[link(wasm_import_module = "env")]
extern "C" {
    /// Log a message to the console
    /// level: 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR
    fn host_log(level: i32, msg_ptr: *const u8, msg_len: i32);

    /// Get an environment variable
    /// Returns: actual length, or negative if not found or buffer too small
    fn host_get_env(
        key_ptr: *const u8,
        key_len: i32,
        value_ptr: *mut u8,
        value_max_len: i32,
    ) -> i32;
}

// ============================================================================
// Helper functions
// ============================================================================

fn log(level: i32, msg: &str) {
    unsafe {
        host_log(level, msg.as_ptr(), msg.len() as i32);
    }
}

fn log_info(msg: &str) {
    log(1, msg);
}

fn get_env(key: &str) -> Option<String> {
    let mut buf = vec![0u8; 256];

    let len = unsafe {
        host_get_env(
            key.as_ptr(),
            key.len() as i32,
            buf.as_mut_ptr(),
            buf.len() as i32,
        )
    };

    if len > 0 {
        buf.truncate(len as usize);
        String::from_utf8(buf).ok()
    } else {
        None
    }
}

// ============================================================================
// Request/Response types (matching host protocol)
// ============================================================================

#[derive(Deserialize)]
struct Request {
    url: String,
    method: String,
    headers: std::collections::HashMap<String, String>,
    body: String,
}

#[derive(Serialize)]
struct Response {
    status: u16,
    headers: std::collections::HashMap<String, String>,
    body: String,
}

// ============================================================================
// Memory management
// ============================================================================

/// Allocate memory for the host to write into
#[no_mangle]
pub extern "C" fn allocate(size: i32) -> *mut u8 {
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    unsafe { alloc(layout) }
}

/// Global response buffer (simple approach for demo)
static mut RESPONSE_BUFFER: Vec<u8> = Vec::new();

// ============================================================================
// Handler exports
// ============================================================================

/// Handle an HTTP fetch event
///
/// Protocol:
/// - Input: JSON string at (ptr, len)
/// - Output: pointer to [4-byte length][JSON response]
#[no_mangle]
pub extern "C" fn handle_fetch(request_ptr: i32, request_len: i32) -> i32 {
    // Read request JSON from memory
    let request_slice =
        unsafe { std::slice::from_raw_parts(request_ptr as *const u8, request_len as usize) };

    let request_str = match std::str::from_utf8(request_slice) {
        Ok(s) => s,
        Err(_) => return write_error_response(400, "Invalid UTF-8 in request"),
    };

    let request: Request = match serde_json::from_str(request_str) {
        Ok(r) => r,
        Err(e) => return write_error_response(400, &format!("Invalid JSON: {}", e)),
    };

    // Log the request
    log_info(&format!(
        "Received {} request to {}",
        request.method, request.url
    ));

    // Get greeting from env or use default
    let greeting = get_env("GREETING").unwrap_or_else(|| "Hello".to_string());

    // Build response
    let mut headers = std::collections::HashMap::new();
    headers.insert("Content-Type".to_string(), "text/plain".to_string());
    headers.insert("X-Powered-By".to_string(), "OpenWorkers-WASM".to_string());

    let response = Response {
        status: 200,
        headers,
        body: format!("{} from Rust WASM! 🦀\nYou requested: {}", greeting, request.url),
    };

    write_response(&response)
}

/// Handle a scheduled event
#[no_mangle]
pub extern "C" fn handle_scheduled(scheduled_time: u64) -> i32 {
    log_info(&format!("Scheduled event at timestamp: {}", scheduled_time));
    0 // Success
}

// ============================================================================
// Response helpers
// ============================================================================

fn write_response(response: &Response) -> i32 {
    let json = serde_json::to_string(response).unwrap_or_else(|_| {
        r#"{"status":500,"headers":{},"body":"Serialization error"}"#.to_string()
    });

    write_response_bytes(json.as_bytes())
}

fn write_error_response(status: u16, message: &str) -> i32 {
    let response = Response {
        status,
        headers: std::collections::HashMap::new(),
        body: message.to_string(),
    };

    write_response(&response)
}

fn write_response_bytes(json_bytes: &[u8]) -> i32 {
    unsafe {
        // Format: [4-byte length (little-endian)][JSON bytes]
        let len = json_bytes.len() as u32;
        RESPONSE_BUFFER.clear();
        RESPONSE_BUFFER.extend_from_slice(&len.to_le_bytes());
        RESPONSE_BUFFER.extend_from_slice(json_bytes);

        RESPONSE_BUFFER.as_ptr() as i32
    }
}
