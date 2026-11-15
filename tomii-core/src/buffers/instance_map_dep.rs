//! Per-instance dependency counters for nodes with an `instance_map`
//! predecessor (see [`crate::instance_map`]).
//!
//! Unlike [`super::node_dep::NodeDependencyEntry`] — which pools decrements into
//! one (or a handful of grouped) shared counters and releases a RANGE of
//! instances together, assuming every instance in that range needs the same
//! THRESHOLD and that arrival order alone determines which ordinal slice is
//! ready — [`InstanceThresholdEntry`] gives every consumer instance its OWN
//! independent counter, seeded with its OWN (possibly different) threshold.
//! This is what an arbitrary per-instance map requires: two instances of the
//! same node can legitimately need a different number of (and different)
//! predecessor cells.
//!
//! The generational lazy-reinit pattern is identical to `NodeDependencyEntry`
//! (see `buffers/mod.rs` module docs): every counter is a packed
//! `(generation, remaining)` `AtomicU64`; a stale generation is lazily treated
//! as the instance's own initial threshold, so per-frame reset is O(1) (bump
//! the slot generation) with no sweep.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{gen_pack, gen_unpack_gen, gen_unpack_val};

/// Per-slot, per-(instance-map node) counter array: one independent,
/// generationally-tagged remaining-count per consumer instance.
#[derive(Debug)]
pub struct InstanceThresholdEntry {
    /// Packed `(gen, remaining)` per consumer instance — length = node factor.
    remaining: Vec<AtomicU64>,
    /// Initial threshold per instance (used for lazy reinit on a stale generation).
    thresholds: Vec<u32>,
}

impl InstanceThresholdEntry {
    pub fn new(thresholds: Vec<u32>) -> Self {
        let remaining = thresholds
            .iter()
            .map(|&t| AtomicU64::new(gen_pack(0, t)))
            .collect();
        Self {
            remaining,
            thresholds,
        }
    }

    /// Decrement every listed instance index by `amount` and collect any
    /// newly-ready indices into `ready` (cleared first).
    ///
    /// `indices` may repeat an instance index (e.g. a bulk completion spanning
    /// several producer cells that all feed the same consumer instance); each
    /// occurrence is an independent `-amount` decrement, exactly mirroring how
    /// many decrements the routing table actually sends for that occurrence.
    /// An instance can only ever transition ready→ready-again once per
    /// generation (the `old > 0 && new == 0` guard below), so duplicate
    /// listings never push the same now-ready index twice into `ready`.
    pub fn decrease_many_and_get_ready_into(
        &self,
        slot_gen: u32,
        indices: &[u32],
        amount: u32,
        ready: &mut Vec<usize>,
    ) {
        ready.clear();
        for &idx in indices {
            let i = idx as usize;
            let Some(slot) = self.remaining.get(i) else {
                continue;
            };
            let init_val = self.thresholds[i];
            #[allow(deprecated)]
            let prev = slot
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                    let g = gen_unpack_gen(packed);
                    let cur = if g == slot_gen {
                        gen_unpack_val(packed)
                    } else {
                        init_val
                    };
                    Some(gen_pack(slot_gen, cur.saturating_sub(amount)))
                })
                .unwrap();
            let g = gen_unpack_gen(prev);
            let old_val = if g == slot_gen {
                gen_unpack_val(prev)
            } else {
                init_val
            };
            let new_val = old_val.saturating_sub(amount);
            if old_val > 0 && new_val == 0 {
                ready.push(i);
            }
        }
    }

    /// Decrement EVERY instance by `amount` (broadcast — an ordinary
    /// barrier/dep/factor-1 predecessor coexisting with a mapped one) and
    /// collect newly-ready indices into `ready` (cleared first). Avoids
    /// allocating a `0..factor` index list for the common broadcast case.
    pub fn decrease_broadcast_and_get_ready_into(
        &self,
        slot_gen: u32,
        amount: u32,
        ready: &mut Vec<usize>,
    ) {
        ready.clear();
        for (i, slot) in self.remaining.iter().enumerate() {
            let init_val = self.thresholds[i];
            #[allow(deprecated)]
            let prev = slot
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |packed| {
                    let g = gen_unpack_gen(packed);
                    let cur = if g == slot_gen {
                        gen_unpack_val(packed)
                    } else {
                        init_val
                    };
                    Some(gen_pack(slot_gen, cur.saturating_sub(amount)))
                })
                .unwrap();
            let g = gen_unpack_gen(prev);
            let old_val = if g == slot_gen {
                gen_unpack_val(prev)
            } else {
                init_val
            };
            let new_val = old_val.saturating_sub(amount);
            if old_val > 0 && new_val == 0 {
                ready.push(i);
            }
        }
    }

    /// Eagerly reinitialise every counter to `new_gen`. Only called at the
    /// once-per-2^32 slot-generation wrap (see `runtime::generation`), never
    /// on the per-frame path, which stays fully lazy.
    pub fn reset_epoch(&self, new_gen: u32) {
        for (slot, &t) in self.remaining.iter().zip(self.thresholds.iter()) {
            slot.store(gen_pack(new_gen, t), Ordering::SeqCst);
        }
    }
}

/// Per-node dependency map for instance-map nodes, using the same 2D
/// slot×node indexing convention as [`super::node_dep::NodeDepMap`]. A node
/// with no `instance_map` predecessor has `None` at its index — the
/// overwhelming majority of nodes, so this map stays sparse.
#[derive(Debug)]
pub struct InstanceMapDepMap {
    slots: Vec<Vec<Option<InstanceThresholdEntry>>>,
}

impl InstanceMapDepMap {
    /// `node_thresholds[node_id] == Some(thresholds)` for every node with an
    /// `instance_map` predecessor; `None` otherwise.
    pub fn new(slots: usize, node_thresholds: &[Option<std::sync::Arc<Vec<u32>>>]) -> Self {
        let map_slots = (0..slots)
            .map(|_| {
                node_thresholds
                    .iter()
                    .map(|t| t.as_ref().map(|v| InstanceThresholdEntry::new(v.to_vec())))
                    .collect()
            })
            .collect();
        Self { slots: map_slots }
    }

    #[inline]
    pub fn decrease_many_and_get_ready_into(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        indices: &[u32],
        amount: u32,
        ready: &mut Vec<usize>,
    ) {
        if let Some(Some(entry)) = self.slots.get(slot).and_then(|s| s.get(node_id)) {
            entry.decrease_many_and_get_ready_into(slot_gen, indices, amount, ready);
        } else {
            ready.clear();
        }
    }

    #[inline]
    pub fn decrease_broadcast_and_get_ready_into(
        &self,
        slot: usize,
        node_id: usize,
        slot_gen: u32,
        amount: u32,
        ready: &mut Vec<usize>,
    ) {
        if let Some(Some(entry)) = self.slots.get(slot).and_then(|s| s.get(node_id)) {
            entry.decrease_broadcast_and_get_ready_into(slot_gen, amount, ready);
        } else {
            ready.clear();
        }
    }

    /// Eagerly reinitialise every `InstanceThresholdEntry` in `slot` to `new_gen`.
    pub fn reset_slot_epoch(&self, slot: usize, new_gen: u32) {
        if let Some(entries) = self.slots.get(slot) {
            for entry in entries.iter().flatten() {
                entry.reset_epoch(new_gen);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn decrease_many_fires_exact_listed_instances() {
        // 3 instances, thresholds [2, 1, 3]
        let entry = InstanceThresholdEntry::new(vec![2, 1, 3]);
        let mut ready = Vec::new();
        entry.decrease_many_and_get_ready_into(0, &[1], 1, &mut ready);
        assert_eq!(ready, vec![1]); // threshold 1, one decrement -> ready
        entry.decrease_many_and_get_ready_into(0, &[0], 1, &mut ready);
        assert!(ready.is_empty()); // threshold 2, one decrement -> not yet
        entry.decrease_many_and_get_ready_into(0, &[0], 1, &mut ready);
        assert_eq!(ready, vec![0]);
        entry.decrease_many_and_get_ready_into(0, &[2, 2, 2], 1, &mut ready);
        assert_eq!(ready, vec![2]); // threshold 3, three decrements in one call
    }

    #[test]
    fn decrease_broadcast_decrements_every_instance() {
        let entry = InstanceThresholdEntry::new(vec![1, 1, 1]);
        let mut ready = Vec::new();
        entry.decrease_broadcast_and_get_ready_into(0, 1, &mut ready);
        let mut r = ready.clone();
        r.sort_unstable();
        assert_eq!(r, vec![0, 1, 2]);
    }

    #[test]
    fn stale_generation_lazily_reinitialises() {
        let entry = InstanceThresholdEntry::new(vec![1]);
        let mut ready = Vec::new();
        entry.decrease_many_and_get_ready_into(0, &[0], 1, &mut ready);
        assert_eq!(ready, vec![0]);
        // New generation: the stale packed (gen=0, val=0) must be treated as
        // the fresh threshold (1), not the stale 0.
        entry.decrease_many_and_get_ready_into(1, &[0], 1, &mut ready);
        assert_eq!(ready, vec![0]);
    }

    #[test]
    fn reset_epoch_restores_thresholds() {
        let entry = InstanceThresholdEntry::new(vec![5]);
        let mut ready = Vec::new();
        entry.decrease_many_and_get_ready_into(0, &[0], 3, &mut ready);
        entry.reset_epoch(7);
        entry.decrease_many_and_get_ready_into(7, &[0], 5, &mut ready);
        assert_eq!(ready, vec![0]);
    }

    #[test]
    fn map_sparse_for_unmapped_nodes() {
        let thresholds: Vec<Option<Arc<Vec<u32>>>> = vec![None, Some(Arc::new(vec![2, 1]))];
        let map = InstanceMapDepMap::new(2, &thresholds);
        let mut ready = Vec::new();
        // node 0 has no entry: harmless no-op
        map.decrease_many_and_get_ready_into(0, 0, 0, &[0], 1, &mut ready);
        assert!(ready.is_empty());
        // node 1 is mapped
        map.decrease_many_and_get_ready_into(0, 1, 0, &[1], 1, &mut ready);
        assert_eq!(ready, vec![1]);
    }
}
