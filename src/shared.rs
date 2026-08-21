//! Process-wide engine and linker, one per fuel mode.
//!
//! An engine and a fully populated linker cost milliseconds to build and are
//! immutable once built, so every worker of a fuel mode shares one of each.
//! Fuel metering changes the code Cranelift emits, which makes it the only
//! engine setting that forks the cache.

use crate::worker::WasmState;
use crate::worker::engine_config;
use openworkers_core::RuntimeLimits;
use openworkers_core::TerminationReason;
use std::sync::OnceLock;
use std::time::Duration;
use wasmtime::Engine;
use wasmtime::component::HasSelf;
use wasmtime::component::Linker;

/// Interval of the background thread driving epoch interruption; also the
/// granularity of wall-clock and abort checks
pub(crate) const EPOCH_TICK: Duration = Duration::from_millis(10);

static ENGINES: [OnceLock<Result<Engine, String>>; 2] = [OnceLock::new(), OnceLock::new()];
static LINKERS: [OnceLock<Result<Linker<WasmState>, String>>; 2] =
    [OnceLock::new(), OnceLock::new()];

pub(crate) fn metered(limits: &RuntimeLimits) -> bool {
    limits.max_cpu_time_ms > 0
}

/// The engine for a fuel mode; a cheap handle on the shared instance.
pub(crate) fn engine(fuel: bool) -> Result<Engine, TerminationReason> {
    let slot = &ENGINES[usize::from(fuel)];

    slot.get_or_init(|| {
        let limits = RuntimeLimits {
            max_cpu_time_ms: u64::from(fuel),
            ..Default::default()
        };

        let engine = Engine::new(&engine_config(&limits))
            .map_err(|e| format!("Failed to create engine: {}", e))?;

        // One ticker per engine, for the lifetime of the process
        let ticker = engine.clone();

        std::thread::spawn(move || {
            loop {
                std::thread::sleep(EPOCH_TICK);

                ticker.increment_epoch();
            }
        });

        Ok(engine)
    })
    .clone()
    .map_err(TerminationReason::InitializationError)
}

/// The linker for a fuel mode, with the full host surface registered.
pub(crate) fn linker(fuel: bool) -> Result<&'static Linker<WasmState>, TerminationReason> {
    let slot = &LINKERS[usize::from(fuel)];

    slot.get_or_init(|| {
        let engine = engine(fuel).map_err(|e| e.to_string())?;

        let mut linker = Linker::new(&engine);

        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|e| format!("Failed to add WASI to linker: {}", e))?;

        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)
            .map_err(|e| format!("Failed to add wasi:http to linker: {}", e))?;

        wasmtime_wasi_http::p3::add_to_linker(&mut linker)
            .map_err(|e| format!("Failed to add wasi:http 0.3 to linker: {}", e))?;

        // Linked for every guest; one that imports no binding simply never
        // calls them
        crate::bindings::WorkerHost::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state)
            .map_err(|e| format!("Failed to add bindings to linker: {}", e))?;

        Ok(linker)
    })
    .as_ref()
    .map_err(|e| TerminationReason::InitializationError(e.clone()))
}
