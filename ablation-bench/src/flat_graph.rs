/// Graph with flat-indexed state: successor lists are dense `Vec<Vec<u32>>` accessed by
/// integer node ID — a single array load with no pointer traversal.
///
/// Matches SynStream's `Vec<Vec<IdType>>` successor structure (§3.1 Flatten claim).
pub struct FlatGraph {
    /// successors[id] = list of successor node IDs.
    /// Outer Vec is contiguous; inner Vecs are dense (not linked pointers).
    pub successors: Vec<Vec<u32>>,
    /// Initial predecessor count per node (precomputed once at build time).
    pub pred_counts: Vec<u32>,
    pub roots: Vec<u32>,
    pub n_nodes: usize,
}

impl FlatGraph {
    /// Linear chain: node 0 → node 1 → … → node n-1.
    /// Zero intra-stream parallelism: ideal for measuring inter-stream (slot) concurrency.
    pub fn from_chain(n: usize) -> Self {
        let n_nodes = n;
        let mut successors: Vec<Vec<u32>> = vec![vec![]; n_nodes];
        let mut pred_counts = vec![0u32; n_nodes];

        for i in 0..n_nodes.saturating_sub(1) {
            successors[i].push((i + 1) as u32);
            pred_counts[i + 1] += 1;
        }

        let roots = vec![0u32];
        Self { successors, pred_counts, roots, n_nodes }
    }

    pub fn from_wavefront(n: usize) -> Self {
        let n_nodes = n * n;
        let mut successors: Vec<Vec<u32>> = vec![vec![]; n_nodes];
        let mut pred_counts = vec![0u32; n_nodes];

        for i in 0..n {
            for j in 0..n {
                let id = (i * n + j) as u32;
                if i > 0 {
                    successors[(i - 1) * n + j].push(id);
                    pred_counts[id as usize] += 1;
                }
                if j > 0 {
                    successors[i * n + (j - 1)].push(id);
                    pred_counts[id as usize] += 1;
                }
            }
        }

        let roots: Vec<u32> = pred_counts
            .iter()
            .enumerate()
            .filter(|(_, &c)| c == 0)
            .map(|(i, _)| i as u32)
            .collect();

        Self { successors, pred_counts, roots, n_nodes }
    }

    /// O(1) successor lookup: single index into contiguous Vec<Vec<u32>>.
    #[inline(always)]
    pub fn successors(&self, id: usize) -> &[u32] {
        &self.successors[id]
    }

    pub fn pred_counts(&self) -> &[u32] {
        &self.pred_counts
    }
}
