/// Debug logging helpers.
///
/// `print_debug` is a thin compatibility shim around `tracing::debug!`.
/// Callers need not change; log level is controlled via `RUST_LOG` or the
/// tracing subscriber configured in `main.rs`.
///
/// `init_debug` is kept for backward compatibility but is now a no-op —
/// use `RUST_LOG=synstream_core=debug` or the `--debug` flag (which sets
/// the tracing subscriber level) to enable debug output.
#[inline(always)]
pub fn print_debug(msg: impl FnOnce() -> String) {
    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!("{}", msg());
    }
}

/// No-op: tracing level is configured by the subscriber (see `main.rs`).
/// Kept for backward compatibility only.
pub fn init_debug(_debug: bool) {}

pub fn is_debug_enabled() -> bool {
    tracing::enabled!(tracing::Level::DEBUG)
}
