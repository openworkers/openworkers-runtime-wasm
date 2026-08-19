//! CPU metering calibration
//!
//! Wasmtime charges roughly one fuel unit per operation, so what a millisecond
//! of CPU buys is a property of the host machine, not a constant: measure it
//! once instead of guessing.

use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use wasmtime::Config;
use wasmtime::Engine;
use wasmtime::Instance;
use wasmtime::Module;
use wasmtime::Store;

/// Pins the rate instead of measuring it
const RATE_ENV: &str = "OW_WASM_FUEL_PER_MS";

/// Rate used when the calibration cannot run; the order measured on an Apple
/// M5 Max
const FALLBACK_PER_MS: u64 = 25_000_000;

/// Counted loop; its cost per iteration is what the calibration divides by
const CALIBRATION_MODULE: &str = r#"
(module
  (func (export "burn") (param $n i64)
    (loop $again
      (local.set $n (i64.sub (local.get $n) (i64.const 1)))
      (br_if $again (i64.ne (local.get $n) (i64.const 0)))
    )
  )
)
"#;

/// Enough for every sample the calibration takes
const CALIBRATION_FUEL: u64 = 1 << 40;

const START_ITERATIONS: u64 = 3_000_000;

/// Sample below which a scheduling hiccup would dominate the reading
const MIN_SAMPLE: Duration = Duration::from_millis(1);

/// Readings kept, because one taken on a busy machine would cost the process
/// half its CPU budget for as long as it lives
const SAMPLES: u32 = 3;

/// Fuel charged per millisecond of guest CPU on this machine, measured once
/// per process
pub(crate) fn per_ms() -> u64 {
    static RATE: OnceLock<u64> = OnceLock::new();

    *RATE.get_or_init(|| match pinned_rate() {
        Some(rate) => rate,
        None => measure().unwrap_or(FALLBACK_PER_MS),
    })
}

fn pinned_rate() -> Option<u64> {
    std::env::var(RATE_ENV)
        .ok()?
        .parse()
        .ok()
        .filter(|&rate| rate > 0)
}

/// Burn a known loop and divide the fuel it cost by the time it took, growing
/// the loop until a sample is long enough to be worth reading
fn measure() -> Option<u64> {
    let mut config = Config::new();

    // The guest engine has both, and each adds work to every loop
    config.epoch_interruption(true);
    config.consume_fuel(true);

    let engine = Engine::new(&config).ok()?;
    let module = Module::new(&engine, CALIBRATION_MODULE).ok()?;

    let mut iterations = START_ITERATIONS;
    let mut best = 0;
    let mut kept = 0;

    while kept < SAMPLES {
        let (fuel, elapsed) = burn(&engine, &module, iterations)?;

        // Keep the best reading: a descheduled thread only ever reads slower
        // than the machine is, and too low a rate cuts real work short
        best = best.max(fuel * 1_000_000 / elapsed.as_nanos().max(1) as u64);

        match elapsed >= MIN_SAMPLE {
            true => kept += 1,
            false => iterations *= 8,
        }
    }

    Some(best.max(1))
}

fn burn(engine: &Engine, module: &Module, iterations: u64) -> Option<(u64, Duration)> {
    let mut store = Store::new(engine, ());

    store.set_fuel(CALIBRATION_FUEL).ok()?;
    store.set_epoch_deadline(u64::MAX);

    let instance = Instance::new(&mut store, module, &[]).ok()?;
    let burn = instance
        .get_typed_func::<u64, ()>(&mut store, "burn")
        .ok()?;

    let started = Instant::now();

    burn.call(&mut store, iterations).ok()?;

    let elapsed = started.elapsed();

    Some((CALIBRATION_FUEL - store.get_fuel().ok()?, elapsed))
}

#[cfg(test)]
mod tests {
    use super::measure;

    /// A host metering fewer than a million operations per millisecond is not
    /// a machine this runtime serves from
    #[test]
    fn measures_a_plausible_rate() {
        let rate = measure().expect("calibration runs");

        println!("fuel per ms: {rate}");

        assert!(rate > 1_000_000, "fuel per ms: {rate}");
    }
}
