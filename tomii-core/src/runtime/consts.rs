//! Compile-time constants shared across the runtime module.

/// Maximum number of concurrent slots (streams) the runtime supports.
///
/// This limit is enforced by the `u64` completion bitmap (`SlotData::active_bitmap`)
/// and the per-slot generation cache in `drain_and_process_batch_queue`.  Raising it
/// above 64 would require widening those to `u128` or a bit-vector type.
pub(super) const MAX_SLOTS: usize = 64;
