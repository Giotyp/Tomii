use cst_macros::*;
use shared::CmTypes;

mod simple_funcs;

fn test_dummy() {
    execute_function!("examples/functions/wrappers.rs", "dummy_wrap");
}

fn test_task_a_cm() {
    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
        CmTypes::String("add".to_string()),
    ];
    let result = execute_function_args!("examples/functions/wrappers.rs", "task_a_wrap", arg_vec);
    assert_eq!(result, 15);
}

fn test_task_b_cm() {
    let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5)];
    let result = execute_function_args!("examples/functions/wrappers.rs", "task_b_wrap", arg_vec);
    assert_eq!(result, false);
}

fn main() {
    test_dummy();
    test_task_a_cm();
    test_task_b_cm();
}