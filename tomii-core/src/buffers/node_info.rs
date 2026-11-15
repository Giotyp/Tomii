use crate::IdType;

#[derive(Clone)]
pub struct NodeInfo {
    pub id: IdType,
    /// Slot generation at scheduling time. Fits in the 6-byte padding between
    /// id (u16) and slot (usize) with no struct-size increase.
    /// Excluded from PartialEq / Hash / Debug so that result-map lookups and
    /// deduplication logic remain unaffected by generation stamps.
    pub gen: u32,
    pub slot: usize,
    pub index: usize,
    /// Number of consecutive instances this task handles starting at `index`.
    /// 1 = single instance (default). >1 = bulk task covering `index..index+bulk_count`.
    /// Excluded from PartialEq/Hash like `gen` (scheduling metadata, not identity).
    pub bulk_count: usize,
    pub pred_index: usize,
    pub post_node: bool,
}

impl NodeInfo {
    pub fn new(id: IdType, slot: usize, index: usize, pred_index: usize) -> NodeInfo {
        NodeInfo {
            id,
            gen: 0,
            slot,
            index,
            bulk_count: 1,
            pred_index,
            post_node: false,
        }
    }

    pub fn set_post_node(&mut self, post_node: bool) {
        self.post_node = post_node;
    }
}

// Intentionally exclude `gen` from equality and hashing: generation is scheduling
// metadata and must not affect result-map lookups or node deduplication.
impl PartialEq for NodeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.slot == other.slot
            && self.index == other.index
            && self.pred_index == other.pred_index
            && self.post_node == other.post_node
    }
}
impl Eq for NodeInfo {}

impl std::hash::Hash for NodeInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.slot.hash(state);
        self.index.hash(state);
        self.pred_index.hash(state);
        self.post_node.hash(state);
    }
}

impl std::fmt::Debug for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NodeID {{ id: {}, index: {}, bulk_count: {}, slot: {}, post_node: {} }}",
            self.id, self.index, self.bulk_count, self.slot, self.post_node
        )
    }
}

#[cfg(test)]
mod tests {
    use super::NodeInfo;

    /// `NodeInfo` is the per-task struct copied through every scheduler queue
    /// and dispatch path (see the `gen` field's doc comment: it was placed to
    /// fit in existing padding specifically to avoid a struct-size increase).
    /// `$frame_id` (see `crate::runtime::arg_resolution`) is resolved from
    /// `SharedData::slot_data.frame_id[node_info.slot]` using the field
    /// `NodeInfo` already carries — it must add NOTHING to this struct. This
    /// locks the size so a future change that accidentally grows `NodeInfo`
    /// (e.g. to carry a frame id directly) is caught here instead of only
    /// showing up as an unexplained regression in a later benchmark.
    #[test]
    fn size_is_unchanged_by_the_frame_id_feature() {
        assert_eq!(
            std::mem::size_of::<NodeInfo>(),
            40,
            "NodeInfo size changed — if this is intentional, update this assert AND confirm \
             `$frame_id` still adds nothing to it (frame_id is read from \
             SharedData::slot_data.frame_id[slot], not stored on NodeInfo itself)"
        );
    }
}
