use crate::wrappers::*;
use synstream_types::*;

pub fn get_func(func_name: &str) -> Option<CmPtr> {
    match func_name {
        "load_graph"    => Some(load_graph_cm_wrap),
        "create_ranks"  => Some(create_ranks_cm_wrap),
        "get_partition"      => Some(get_partition_cm_wrap),
        "get_all_partitions" => Some(get_all_partitions_cm_wrap),
        "pr_scatter"         => Some(pr_scatter_cm_wrap),
        "pr_gather"          => Some(pr_gather_cm_wrap),
        "pr_partial_gather"  => Some(pr_partial_gather_cm_wrap),
        "pr_reduce"          => Some(pr_reduce_cm_wrap),
        "pr_reduce_partial"  => Some(pr_reduce_partial_cm_wrap),
        _ => {
            println!("Function {} not found", func_name);
            panic!("Panicking...");
        }
    }
}
