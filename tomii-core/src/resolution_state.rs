// Backward-compatibility shim: preserved so that any out-of-tree code that
// imports `tomii_core::resolution_state::{ResolutionState, MultiThreadedState}`
// continues to compile.  All in-tree code now uses `dependency_counter` directly.
#[allow(unused_imports)]
pub use crate::dependency_counter::DependencyCounter as ResolutionState;
#[allow(unused_imports)]
pub use crate::dependency_counter::MultiThreadedCounter as MultiThreadedState;
