/// Graph with pointer-linked topology: each node is a separately heap-allocated object
/// accessed via raw pointer. Models OpenMP TaskGraph record-and-replay and Taskflow's
/// tf::Task objects, which pre-build work units but retain pointer-linked topology.
///
/// Each `PointerNode` is allocated individually (Box::new), so nodes are scattered across
/// the heap. Successor traversal requires dereferencing a raw pointer to a potentially
/// cache-cold memory location — the overhead the Flatten claim addresses.
pub struct PointerNode {
    pub successors: Vec<u32>, // successor IDs stored inside the heap-allocated node
    #[allow(dead_code)]
    pub pred_count: u32,
}

pub struct PointerGraph {
    /// Keeps all PointerNodes alive for the graph's lifetime.
    _storage: Vec<Box<PointerNode>>,
    /// Hot-path access: array of raw pointers to individually heap-allocated nodes.
    /// Dereferencing node_ptrs[id] may cause a cache miss for large N because
    /// each PointerNode is at an independent heap location.
    pub node_ptrs: Vec<*const PointerNode>,
    pub roots: Vec<u32>,
    pub n_nodes: usize,
    pred_counts_cache: Vec<u32>, // parallel copy for reset logic
}

// Safety: PointerGraph is read-only during benchmark execution.
// Raw pointers point to stable Box<PointerNode> heap allocations that live as long as _storage.
unsafe impl Send for PointerGraph {}
unsafe impl Sync for PointerGraph {}

impl PointerGraph {
    pub fn from_wavefront(n: usize) -> Self {
        let n_nodes = n * n;
        let mut succ_lists: Vec<Vec<u32>> = vec![vec![]; n_nodes];
        let mut pred_counts = vec![0u32; n_nodes];

        for i in 0..n {
            for j in 0..n {
                let id = (i * n + j) as u32;
                if i > 0 {
                    succ_lists[(i - 1) * n + j].push(id);
                    pred_counts[id as usize] += 1;
                }
                if j > 0 {
                    succ_lists[i * n + (j - 1)].push(id);
                    pred_counts[id as usize] += 1;
                }
            }
        }

        // Allocate each node as a separate Box (scattered heap locations).
        let storage: Vec<Box<PointerNode>> = (0..n_nodes)
            .map(|id| {
                Box::new(PointerNode {
                    successors: std::mem::take(&mut succ_lists[id]),
                    pred_count: pred_counts[id],
                })
            })
            .collect();

        // Capture raw pointers after storage is fully populated.
        // Box<PointerNode> heap addresses are stable once allocated.
        let node_ptrs: Vec<*const PointerNode> =
            storage.iter().map(|b| b.as_ref() as *const PointerNode).collect();

        let roots: Vec<u32> = pred_counts
            .iter()
            .enumerate()
            .filter(|(_, &c)| c == 0)
            .map(|(i, _)| i as u32)
            .collect();

        let pred_counts_cache = pred_counts;
        Self { _storage: storage, node_ptrs, roots, n_nodes, pred_counts_cache }
    }

    /// Accesses the successor list by dereferencing a raw pointer to a heap-allocated node.
    /// For large N, the PointerNode at `node_ptrs[id]` is likely cache-cold.
    ///
    /// # Safety
    /// `id` must be a valid node index, and `self` must outlive the returned slice.
    #[inline(always)]
    pub fn successors(&self, id: usize) -> &[u32] {
        // SAFETY: node_ptrs[id] points to a live Box<PointerNode> kept in _storage.
        unsafe { &(*self.node_ptrs[id]).successors }
    }

    pub fn pred_counts(&self) -> &[u32] {
        &self.pred_counts_cache
    }
}
