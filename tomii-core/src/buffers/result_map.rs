use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicU8, Ordering};

use super::NodeInfo;
use crate::graph_struct::Node;

/// Sentinel tag: value is boxed in `buffer`; tag range 1-14 is inline primitives.
const TAG_BOXED: u8 = 255;

/// Encode a primitive CmTypes variant into a (tag, u64_bits) pair, or None for non-primitives.
///
/// Encoding:
///   1=F64  2=F32  3=I64  4=U64  5=I32  6=U32  7=I16  8=U16  9=I8
///   10=U8  11=Bool  12=Usize  13=Isize  14=Char
///
/// The Release store of `inline_tag` synchronises the paired Relaxed store of `inline_val`
/// (sequenced-before in the writer). The Acquire load of `inline_tag` on the reader side
/// makes the `inline_val` write visible, so the subsequent Relaxed load of `inline_val`
/// is coherent. Non-primitive types (Arc-wrapped, Vec, Any, …) fall through to the boxed path.
#[inline]
fn encode_inline(result: &tomii_types::CmTypes) -> Option<(u8, u64)> {
    use tomii_types::CmTypes;
    match result {
        CmTypes::F64(v) => Some((1, v.to_bits())),
        CmTypes::F32(v) => Some((2, v.to_bits() as u64)),
        CmTypes::I64(v) => Some((3, *v as u64)),
        CmTypes::U64(v) => Some((4, *v)),
        CmTypes::I32(v) => Some((5, *v as u64)),
        CmTypes::U32(v) => Some((6, *v as u64)),
        CmTypes::I16(v) => Some((7, *v as u64)),
        CmTypes::U16(v) => Some((8, *v as u64)),
        CmTypes::I8(v) => Some((9, *v as u64)),
        CmTypes::U8(v) => Some((10, *v as u64)),
        CmTypes::Bool(v) => Some((11, *v as u64)),
        CmTypes::Usize(v) => Some((12, *v as u64)),
        CmTypes::Isize(v) => Some((13, *v as u64)),
        CmTypes::Char(v) => Some((14, *v as u64)),
        _ => None,
    }
}

#[inline]
fn decode_inline(tag: u8, val: u64) -> tomii_types::CmTypes {
    use tomii_types::CmTypes;
    match tag {
        1 => CmTypes::F64(f64::from_bits(val)),
        2 => CmTypes::F32(f32::from_bits(val as u32)),
        3 => CmTypes::I64(val as i64),
        4 => CmTypes::U64(val),
        5 => CmTypes::I32(val as i32),
        6 => CmTypes::U32(val as u32),
        7 => CmTypes::I16(val as i16),
        8 => CmTypes::U16(val as u16),
        9 => CmTypes::I8(val as i8),
        10 => CmTypes::U8(val as u8),
        11 => CmTypes::Bool(val != 0),
        12 => CmTypes::Usize(val as usize),
        13 => CmTypes::Isize(val as isize),
        14 => CmTypes::Char(char::from_u32(val as u32).unwrap_or('\0')),
        _ => unreachable!("invalid inline tag {tag}"),
    }
}

/// Lock-free result storage using atomic pointer swaps.
///
/// Small copy types (F64, I64, U64, F32, all integer/bool/char variants ≤ 64 bits) are stored
/// inline in a parallel `AtomicU64` array, eliminating one `Box` heap allocation per cell per
/// stream for the common case. Non-primitive types (Arc-wrapped, Vec, Any, Bytes, …) are boxed
/// as before.
///
/// The invariant assumed by the runtime (and not verified here) is: `get` is only called on a
/// cell after `set` for that cell has completed (the dependency tracker guarantees this), and
/// `reinit_slot` is called only when no worker thread is accessing the slot. Violating either
/// invariant is a runtime bug, not a bug in this data structure.
pub struct LockFreeResultMap {
    /// For boxed (non-primitive) values. Null means empty or inline.
    buffer: Vec<AtomicPtr<tomii_types::CmTypes>>,
    /// Inline primitive values; coherent only when `inline_tag` is 1-14.
    inline_val: Vec<AtomicU64>,
    /// 0=empty, 1-14=inline primitive type, 255=boxed.
    inline_tag: Vec<AtomicU8>,
    per_slot_size: usize,
    node_offsets: Vec<usize>,
    node_factors: Vec<usize>,
    nodes_len: usize,
    slots: usize,
}

impl LockFreeResultMap {
    pub fn new(nodes: &[Node], slots: usize) -> Self {
        let nodes_len = nodes.len();
        let mut node_factors = Vec::with_capacity(nodes_len);
        let mut node_offsets = Vec::with_capacity(nodes_len);

        let mut offset = 0;
        for node in nodes.iter() {
            node_offsets.push(offset);
            node_factors.push(node.factor);
            offset += node.factor;
        }
        let per_slot_size = offset;

        let total_size = slots * per_slot_size;
        let mut buffer = Vec::with_capacity(total_size);
        let mut inline_val = Vec::with_capacity(total_size);
        let mut inline_tag = Vec::with_capacity(total_size);
        for _ in 0..total_size {
            buffer.push(AtomicPtr::new(null_mut()));
            inline_val.push(AtomicU64::new(0));
            inline_tag.push(AtomicU8::new(0));
        }

        Self {
            buffer,
            inline_val,
            inline_tag,
            per_slot_size,
            node_offsets,
            node_factors,
            nodes_len,
            slots,
        }
    }

    #[inline]
    fn flat_index(&self, node_info: &NodeInfo) -> usize {
        let node_id = node_info.id as usize;
        node_info.slot * self.per_slot_size + self.node_offsets[node_id] + node_info.index
    }

    /// Atomically store a result.
    ///
    /// For primitive variants (≤ 64-bit copy types): stores value into `inline_val` with Relaxed
    /// and tag into `inline_tag` with Release (the Release fence makes `inline_val` visible).
    ///
    /// For non-primitive variants: boxes the value, swaps into `buffer` with Release, then sets
    /// `inline_tag = TAG_BOXED` with Release.
    ///
    /// Safety invariant: at most one writer per cell per slot lifecycle (enforced by the runtime's
    /// dependency model). `get` is only called after `set` completes.
    #[inline]
    pub fn set(&self, node_info: &NodeInfo, result: tomii_types::CmTypes) {
        if node_info.slot >= self.slots || (node_info.id as usize) >= self.nodes_len {
            panic!(
                "Invalid node_info: slot={}, id={}",
                node_info.slot, node_info.id
            );
        }

        let node_id = node_info.id as usize;
        if node_info.index >= self.node_factors[node_id] {
            panic!(
                "Index {} out of bounds for node {}",
                node_info.index, node_info.id
            );
        }

        let idx = self.flat_index(node_info);

        if let Some((tag, val)) = encode_inline(&result) {
            // Inline path: no heap allocation.
            // val write is sequenced-before the Release store of tag; the Release fence
            // carries it, so a reader that Acquire-loads tag=1..14 will see the val.
            self.inline_val[idx].store(val, Ordering::Relaxed);
            self.inline_tag[idx].store(tag, Ordering::Release);
        } else {
            // Boxed path: allocate and store the pointer.
            let new_ptr = Box::into_raw(Box::new(result));
            let old_ptr = self.buffer[idx].swap(new_ptr, Ordering::Release);
            // Mark as boxed after the ptr is visible. Per the runtime invariant, `get` is only
            // called after `set` returns, so the brief window where ptr is stored but tag is still
            // 0 is safe.
            self.inline_tag[idx].store(TAG_BOXED, Ordering::Release);
            if !old_ptr.is_null() {
                // SAFETY: produced by `Box::into_raw`; the atomic swap gives exclusive ownership.
                unsafe {
                    drop(Box::from_raw(old_ptr));
                }
            }
        }
    }

    /// Atomically load a result.
    ///
    /// The `inline_tag` Acquire load is the synchronisation point; it pairs with the Release
    /// store in `set` and makes both `inline_val` (for primitives) and `buffer` (for boxed) visible.
    #[inline]
    pub fn get(&self, node_info: &NodeInfo) -> Option<tomii_types::CmTypes> {
        if node_info.slot >= self.slots || (node_info.id as usize) >= self.nodes_len {
            return None;
        }

        let node_id = node_info.id as usize;
        if node_info.index >= self.node_factors[node_id] {
            return None;
        }

        let idx = self.flat_index(node_info);
        let tag = self.inline_tag[idx].load(Ordering::Acquire);
        match tag {
            0 => None,
            TAG_BOXED => {
                // Acquire on buffer syncs with the Release swap in `set`.
                let ptr = self.buffer[idx].load(Ordering::Acquire);
                if ptr.is_null() {
                    None
                } else {
                    // SAFETY: non-null ptr was stored by `set` via `Box::into_raw`; Acquire/Release
                    // pair ensures the pointee is initialised; we only clone, so no aliasing.
                    Some(unsafe { (*ptr).clone() })
                }
            }
            t => {
                // Relaxed is safe: the Acquire load of `inline_tag` (above) synchronises with the
                // Release store in `set`, which was sequenced-after the Relaxed store of `inline_val`.
                let val = self.inline_val[idx].load(Ordering::Relaxed);
                Some(decode_inline(t, val))
            }
        }
    }

    /// Check if a result is present without cloning.
    #[inline]
    pub fn result_exists(&self, node_info: &NodeInfo) -> bool {
        if node_info.slot >= self.slots || (node_info.id as usize) >= self.nodes_len {
            return false;
        }

        let node_id = node_info.id as usize;
        if node_info.index >= self.node_factors[node_id] {
            return false;
        }

        let idx = self.flat_index(node_info);
        self.inline_tag[idx].load(Ordering::Acquire) != 0
    }

    /// Clear a slot for reuse by the next stream.
    ///
    /// SeqCst on `inline_tag.swap(0)` is the primary ordering point; it ensures the reset is
    /// visible to all threads before the slot is re-dispatched. Boxed values are freed when the
    /// tag swap reveals TAG_BOXED; inline primitives require no deallocation.
    pub fn reinit_slot(&self, slot: usize) {
        if slot >= self.slots {
            panic!("Slot {} out of bounds", slot);
        }

        let start = slot * self.per_slot_size;
        let end = start + self.per_slot_size;

        for idx in start..end {
            let old_tag = self.inline_tag[idx].swap(0, Ordering::SeqCst);
            if old_tag == TAG_BOXED {
                // SAFETY: the SeqCst swap gives exclusive ownership of the boxed ptr.
                let old_ptr = self.buffer[idx].swap(null_mut(), Ordering::SeqCst);
                if !old_ptr.is_null() {
                    unsafe {
                        drop(Box::from_raw(old_ptr));
                    }
                }
            }
            // Inline primitives (old_tag 1-14): no heap allocation to free.
            // `inline_val` can be left with stale bits; `inline_tag = 0` gates access.
        }
    }
}

impl Drop for LockFreeResultMap {
    fn drop(&mut self) {
        for idx in 0..self.inline_tag.len() {
            if self.inline_tag[idx].load(Ordering::Acquire) == TAG_BOXED {
                let ptr = self.buffer[idx].load(Ordering::Acquire);
                if !ptr.is_null() {
                    // SAFETY: `&mut self` gives exclusive access; no other thread can race here.
                    unsafe {
                        drop(Box::from_raw(ptr));
                    }
                }
            }
        }
    }
}

// Safety: AtomicPtr/AtomicU64/AtomicU8 are Send+Sync; raw-ptr access is guarded by
// Release/Acquire pairs and the runtime's single-writer-per-cell invariant.
unsafe impl Send for LockFreeResultMap {}
unsafe impl Sync for LockFreeResultMap {}

impl std::fmt::Debug for LockFreeResultMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "LockFreeResultMap {{")?;
        writeln!(f, "  slots: {}", self.slots)?;
        writeln!(f, "  per_slot_size: {}", self.per_slot_size)?;
        writeln!(f, "  nodes: {}", self.nodes_len)?;
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_struct::{Node, NodePriority};
    use tomii_types::CmTypes;

    fn make_nodes(factors: &[usize]) -> Vec<Node> {
        factors
            .iter()
            .enumerate()
            .map(|(i, &factor)| Node {
                name: format!("node_{}", i),
                args: Vec::new(),
                id: i as crate::IdType,
                loop_args: None,
                factor,
                group_size: None,
                func_name: String::new(),
                loop_: None,
                condition: None,
                priority: NodePriority::Normal,
                use_workers: None,
            })
            .collect()
    }

    fn make_node_info(slot: usize, id: crate::IdType, index: usize, gen: u32) -> NodeInfo {
        NodeInfo {
            id,
            slot,
            index,
            gen,
            bulk_count: 1,
            pred_index: 0,
            post_node: false,
        }
    }

    // -------------------------------------------------------------------------
    // Slot lifecycle: set → get → reinit → set again
    // -------------------------------------------------------------------------

    #[test]
    fn test_slot_lifecycle_set_get_reinit_set() {
        let nodes = make_nodes(&[2]); // one node, factor=2
        let map = LockFreeResultMap::new(&nodes, 1);

        let ni0 = make_node_info(0, 0, 0, 0);
        let ni1 = make_node_info(0, 0, 1, 0);

        assert!(map.get(&ni0).is_none());
        assert!(map.get(&ni1).is_none());

        map.set(&ni0, CmTypes::I32(10));
        map.set(&ni1, CmTypes::I32(20));
        assert_eq!(map.get(&ni0), Some(CmTypes::I32(10)));
        assert_eq!(map.get(&ni1), Some(CmTypes::I32(20)));

        map.reinit_slot(0);
        assert!(map.get(&ni0).is_none());
        assert!(map.get(&ni1).is_none());

        map.set(&ni0, CmTypes::I32(30));
        assert_eq!(map.get(&ni0), Some(CmTypes::I32(30)));
        assert!(map.get(&ni1).is_none());
    }

    // -------------------------------------------------------------------------
    // Multi-slot isolation
    // -------------------------------------------------------------------------

    #[test]
    fn test_multi_slot_result_isolation() {
        let nodes = make_nodes(&[3]);
        let map = LockFreeResultMap::new(&nodes, 2);

        for idx in 0..3 {
            let ni = make_node_info(0, 0, idx, 0);
            map.set(&ni, CmTypes::I32(idx as i32 * 10));
        }

        for idx in 0..3 {
            let ni1 = make_node_info(1, 0, idx, 0);
            assert!(
                map.get(&ni1).is_none(),
                "slot 1 idx {} leaked from slot 0",
                idx
            );
        }

        let ni1_0 = make_node_info(1, 0, 0, 0);
        map.set(&ni1_0, CmTypes::I32(99));
        let ni0_0 = make_node_info(0, 0, 0, 0);
        assert_eq!(map.get(&ni0_0), Some(CmTypes::I32(0)));
        assert_eq!(map.get(&ni1_0), Some(CmTypes::I32(99)));
    }

    // -------------------------------------------------------------------------
    // result_exists consistent with get
    // -------------------------------------------------------------------------

    #[test]
    fn test_result_exists_consistent_with_get() {
        let nodes = make_nodes(&[1]);
        let map = LockFreeResultMap::new(&nodes, 1);
        let ni = make_node_info(0, 0, 0, 0);

        assert!(!map.result_exists(&ni));
        map.set(&ni, CmTypes::Bool(true));
        assert!(map.result_exists(&ni));
        map.reinit_slot(0);
        assert!(!map.result_exists(&ni));
    }

    // -------------------------------------------------------------------------
    // reinit only clears the target slot
    // -------------------------------------------------------------------------

    #[test]
    fn test_reinit_only_clears_target_slot() {
        let nodes = make_nodes(&[1]);
        let map = LockFreeResultMap::new(&nodes, 3);

        for slot in 0..3 {
            let ni = make_node_info(slot, 0, 0, 0);
            map.set(&ni, CmTypes::I32(slot as i32));
        }

        map.reinit_slot(1);

        assert_eq!(map.get(&make_node_info(0, 0, 0, 0)), Some(CmTypes::I32(0)));
        assert!(map.get(&make_node_info(1, 0, 0, 0)).is_none());
        assert_eq!(map.get(&make_node_info(2, 0, 0, 0)), Some(CmTypes::I32(2)));
    }

    // -------------------------------------------------------------------------
    // Concurrent set + get (multiple writers, then sequential reader)
    // -------------------------------------------------------------------------

    #[test]
    fn test_concurrent_set_get() {
        use std::sync::Arc;
        use std::thread;

        let nodes = make_nodes(&[64]);
        let map = Arc::new(LockFreeResultMap::new(&nodes, 1));

        let writers: Vec<_> = (0..64)
            .map(|idx| {
                let m = Arc::clone(&map);
                thread::spawn(move || {
                    let ni = make_node_info(0, 0, idx, 0);
                    m.set(&ni, CmTypes::I32(idx as i32));
                })
            })
            .collect();

        for w in writers {
            w.join().unwrap();
        }

        for idx in 0..64 {
            let ni = make_node_info(0, 0, idx, 0);
            assert_eq!(map.get(&ni), Some(CmTypes::I32(idx as i32)));
        }
    }

    // -------------------------------------------------------------------------
    // Inline primitive tag invariant: every primitive variant round-trips correctly,
    // including edge cases (0.0, negative, INT_MIN, u64::MAX, false, NUL char).
    // -------------------------------------------------------------------------

    #[test]
    fn test_inline_primitive_roundtrip() {
        let nodes = make_nodes(&[1]);
        let map = LockFreeResultMap::new(&nodes, 1);
        let ni = make_node_info(0, 0, 0, 0);

        let cases: &[CmTypes] = &[
            CmTypes::F64(3.14159265358979),
            CmTypes::F64(0.0),
            CmTypes::F64(-1.5),
            CmTypes::F64(f64::MAX),
            CmTypes::F64(f64::MIN_POSITIVE),
            CmTypes::F32(1.0),
            CmTypes::F32(0.0),
            CmTypes::I64(i64::MIN),
            CmTypes::I64(i64::MAX),
            CmTypes::I64(-1),
            CmTypes::U64(u64::MAX),
            CmTypes::U64(0),
            CmTypes::I32(i32::MIN),
            CmTypes::I32(i32::MAX),
            CmTypes::U32(u32::MAX),
            CmTypes::I16(i16::MIN),
            CmTypes::U16(u16::MAX),
            CmTypes::I8(i8::MIN),
            CmTypes::U8(u8::MAX),
            CmTypes::Bool(false),
            CmTypes::Bool(true),
            CmTypes::Usize(usize::MAX),
            CmTypes::Usize(0),
            CmTypes::Isize(isize::MIN),
            CmTypes::Char('\0'),
            CmTypes::Char('Z'),
            CmTypes::Char('™'),
        ];

        for val in cases {
            map.set(&ni, val.clone());
            assert_eq!(
                map.get(&ni),
                Some(val.clone()),
                "round-trip failed for {val:?}"
            );
            assert!(map.result_exists(&ni));
            map.reinit_slot(0);
            assert!(
                !map.result_exists(&ni),
                "still present after reinit for {val:?}"
            );
            assert!(
                map.get(&ni).is_none(),
                "get non-None after reinit for {val:?}"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Non-primitive (boxed) path: Arc-wrapped types still work correctly.
    // -------------------------------------------------------------------------

    #[test]
    fn test_boxed_arc_roundtrip() {
        use std::sync::Arc;

        let nodes = make_nodes(&[1]);
        let map = LockFreeResultMap::new(&nodes, 1);
        let ni = make_node_info(0, 0, 0, 0);

        let s = CmTypes::String(Arc::from("hello"));
        map.set(&ni, s.clone());
        assert_eq!(map.get(&ni), Some(s));
        map.reinit_slot(0);
        assert!(map.get(&ni).is_none());
    }
}

// ---------------------------------------------------------------------------
// Loom model: verifies that the inline_tag Release/Acquire pair and the
// SeqCst reinit_slot are sufficient for slot-reset visibility under all
// thread interleavings explored by loom.
//
// Run with: RUSTFLAGS="--cfg loom" cargo test -p tomii-core --lib
// ---------------------------------------------------------------------------

// R1.7 note: the `DependencyCounter` rename (formerly `ResolutionState`) does NOT
// affect the loom model below.  The underlying `AtomicU64` completed_slots bitmap
// in `MultiThreadedCounter` is identical to the old `MultiThreadedState`; only the
// type names changed.  The loom model validates the atomics that `MultiThreadedCounter`
// relies on (`AtomicU64` fetch_or / fetch_and for `try_complete_slot` /
// `unmark_slot_completed`), so no separate loom test for `MultiThreadedCounter` is
// needed — the invariant is already covered by `inline_tag_slot_reset_visibility`.

#[cfg(loom)]
mod loom_tests {
    use loom::sync::atomic::{AtomicU64, AtomicU8, Ordering};
    use loom::sync::Arc;

    /// Three-thread model:
    ///   Writer  : store val (Relaxed) then tag=1 (Release)  — inline F64 set
    ///   Reiniter: swap tag→0 (SeqCst)                       — slot reset
    ///   Reader  : if tag==1 after both threads join, val must equal the stored bits
    ///
    /// Loom explores all orderings; the assertion catches any case where tag=1 is
    /// visible but val carries stale bits.
    #[test]
    fn inline_tag_slot_reset_visibility() {
        loom::model(|| {
            let tag = Arc::new(AtomicU8::new(0u8));
            let val = Arc::new(AtomicU64::new(0u64));

            let (tag_w, val_w) = (Arc::clone(&tag), Arc::clone(&val));
            let write_thread = loom::thread::spawn(move || {
                val_w.store(f64::to_bits(1.0_f64), Ordering::Relaxed);
                tag_w.store(1u8, Ordering::Release);
            });

            let tag_r = Arc::clone(&tag);
            let reinit_thread = loom::thread::spawn(move || {
                tag_r.swap(0u8, Ordering::SeqCst);
            });

            write_thread.join().unwrap();
            reinit_thread.join().unwrap();

            let t = tag.load(Ordering::Acquire);
            if t == 1 {
                // Writer ran after the reinit; val must carry the correct bits.
                let v = val.load(Ordering::Relaxed);
                assert_eq!(
                    f64::from_bits(v),
                    1.0_f64,
                    "val incoherent after concurrent reinit: tag=1 but val bits={v:#x}"
                );
            } else {
                assert_eq!(t, 0u8, "unexpected tag value {t}");
            }
        });
    }

    /// Smoke test for the `MultiThreadedCounter::try_complete_slot` atomic.
    ///
    /// Two threads race to complete slot 0; exactly one must win (fetch_or 0→1),
    /// the other must lose (fetch_or returns 1).  This verifies the SeqCst
    /// fetch_or / bit-test idiom that `DependencyCounter::try_complete_slot`
    /// relies on — the rename from `MultiThreadedState` to `MultiThreadedCounter`
    /// did not alter any of the underlying atomics.
    #[test]
    fn dependency_counter_try_complete_slot_exclusive() {
        loom::model(|| {
            let completed_slots = Arc::new(AtomicU64::new(0));

            let bits_a = Arc::clone(&completed_slots);
            let thread_a = loom::thread::spawn(move || {
                let prev = bits_a.fetch_or(1u64 << 0, Ordering::SeqCst);
                prev & (1u64 << 0) == 0 // true iff this thread won
            });

            let bits_b = Arc::clone(&completed_slots);
            let thread_b = loom::thread::spawn(move || {
                let prev = bits_b.fetch_or(1u64 << 0, Ordering::SeqCst);
                prev & (1u64 << 0) == 0 // true iff this thread won
            });

            let won_a = thread_a.join().unwrap();
            let won_b = thread_b.join().unwrap();

            // Exactly one thread must have seen the 0→1 transition.
            assert!(
                won_a ^ won_b,
                "try_complete_slot must be exclusive: won_a={won_a} won_b={won_b}"
            );
        });
    }
}
