mod node_info;
mod node_dep;
mod result_map;

pub use node_info::*;
pub use node_dep::*;
pub use result_map::*;

// ---------------------------------------------------------------------------
// Generational pack/unpack helpers
//
// We pack a 32-bit generation counter and a 32-bit value into a single u64.
// Upper 32 bits = generation, lower 32 bits = value (remaining count or sent flag).
// This lets a single SeqCst CAS atomically reset a counter when a new generation
// starts (lazy reinit), eliminating the O(nodes × factor) slot reset loops.
// ---------------------------------------------------------------------------

/// Pack generation `gen` and value `val` into a single u64.
#[inline(always)]
pub fn gen_pack(gen: u32, val: u32) -> u64 {
    ((gen as u64) << 32) | (val as u64)
}

/// Extract the generation from a packed u64.
#[inline(always)]
pub fn gen_unpack_gen(packed: u64) -> u32 {
    (packed >> 32) as u32
}

/// Extract the value from a packed u64.
#[inline(always)]
pub fn gen_unpack_val(packed: u64) -> u32 {
    packed as u32
}
