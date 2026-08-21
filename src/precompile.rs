//! Ahead-of-time compilation of guest components.
//!
//! Compiling a component with Cranelift takes tens of milliseconds and happens
//! again on every cold start. [`precompile`] pays that cost once and hands back
//! the machine code; [`WasmWorker::new_precompiled`] loads it in microseconds.
//!
//! [`WasmWorker::new_precompiled`]: crate::WasmWorker::new_precompiled

use crate::worker::engine_config;
use openworkers_core::RuntimeLimits;
use openworkers_core::TerminationReason;
use std::hash::Hash;
use std::hash::Hasher;
use wasmtime::Engine;

/// First four bytes of every WebAssembly binary, module and component alike
pub(crate) const WASM_MAGIC: &[u8; 4] = b"\0asm";

/// A component compiled by [`precompile`], ready to load without compiling.
///
/// # Trust contract
///
/// Loading one of these calls `wasmtime::component::Component::deserialize`,
/// which validates nothing: it maps the bytes in and jumps into them. Only the
/// output of [`precompile`] qualifies, whether it comes back immediately or
/// through a cache the host itself filled with it; bytes a tenant can reach
/// are native code of its choosing running as the host. Guest bytes go to
/// `WasmWorker::new`, which compiles them.
///
/// Wasmtime does check the engine configuration stored in the artifact, but
/// that only rules out a stale artifact, not a forged one.
pub struct PrecompiledComponent {
    bytes: Vec<u8>,
}

impl PrecompiledComponent {
    /// Take ownership of an artifact, vouching for where it came from.
    ///
    /// # Safety
    ///
    /// `bytes` must be output of [`precompile`], unmodified, and must not have
    /// passed through anything a tenant controls. See the trust contract on
    /// this type.
    pub unsafe fn from_trusted_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// The artifact, for a host that wants to store or forward it
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Compile a component into the artifact the runtime can load without
/// compiling again.
///
/// `limits` must be the limits the worker will run under: a CPU budget turns
/// on fuel metering, which changes the code Cranelift emits.
///
/// See the trust contract on [`PrecompiledComponent`] before storing the
/// result anywhere a tenant can write.
pub fn precompile(
    wasm: &[u8],
    limits: Option<RuntimeLimits>,
) -> Result<Vec<u8>, TerminationReason> {
    check_wasm_magic(wasm)?;

    let engine = build_engine(limits)?;

    engine.precompile_component(wasm).map_err(|e| {
        TerminationReason::InitializationError(format!("Failed to precompile component: {}", e))
    })
}

/// Key naming which artifacts this build can load.
///
/// Two runtimes reporting the same key produce interchangeable artifacts, so a
/// cache keyed on it never hands back bytes the engine would refuse. The key
/// covers everything that goes into compilation: the wasmtime version, the
/// target and its CPU features, and the engine settings `limits` selects.
pub fn compatibility_key(limits: Option<RuntimeLimits>) -> Result<String, TerminationReason> {
    let engine = build_engine(limits)?;

    let mut hasher = Fnv1a::new();

    engine.precompile_compatibility_hash().hash(&mut hasher);

    Ok(format!("{:016x}", hasher.finish()))
}

/// Reject anything that is not a WebAssembly binary, so the only way to reach
/// `Component::deserialize` stays the explicit precompiled constructor. Note
/// that this also rules out the text format, which `Component::new` would
/// otherwise accept.
pub(crate) fn check_wasm_magic(bytes: &[u8]) -> Result<(), TerminationReason> {
    if bytes.starts_with(WASM_MAGIC) {
        return Ok(());
    }

    Err(TerminationReason::InitializationError(
        "worker code is not a WebAssembly binary: it does not start with the \\0asm magic"
            .to_string(),
    ))
}

fn build_engine(limits: Option<RuntimeLimits>) -> Result<Engine, TerminationReason> {
    let config = engine_config(&limits.unwrap_or_default());

    Engine::new(&config).map_err(|e| {
        TerminationReason::InitializationError(format!("Failed to create engine: {}", e))
    })
}

/// FNV-1a, because the compatibility key travels between processes and
/// `DefaultHasher` is not promised to agree across Rust releases.
struct Fnv1a(u64);

impl Fnv1a {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for Fnv1a {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_compatibility_key_is_stable_across_calls() {
        let first = compatibility_key(None).expect("engine should build");
        let second = compatibility_key(None).expect("engine should build");

        assert_eq!(first, second);
    }

    /// Fuel metering changes the emitted code, so the two configurations must
    /// not share a cache entry
    #[test]
    fn metered_and_unmetered_limits_have_different_keys() {
        let metered = RuntimeLimits {
            max_cpu_time_ms: 100,
            ..Default::default()
        };

        let unmetered = RuntimeLimits {
            max_cpu_time_ms: 0,
            ..Default::default()
        };

        assert_ne!(
            compatibility_key(Some(metered)).expect("engine should build"),
            compatibility_key(Some(unmetered)).expect("engine should build")
        );
    }

    #[test]
    fn precompiling_non_wasm_bytes_fails() {
        let error = precompile(b"\x7fELF not a component", None).expect_err("ELF is not wasm");

        assert!(
            matches!(&error, TerminationReason::InitializationError(msg) if msg.contains("\\0asm")),
            "unexpected error: {error:?}"
        );
    }
}
