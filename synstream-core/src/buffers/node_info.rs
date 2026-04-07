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
