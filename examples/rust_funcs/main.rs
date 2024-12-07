use cst_macros::*;
use shared::CmTypes;

mod simple_funcs;
mod wrappers;
mod func_reg;

use func_reg::*;

fn test_dummy() {
    execute_function!("examples/rust_funcs/wrappers.rs", "dummy_wrap");
}

fn test_task_a_cm() {
    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
        CmTypes::String("add".to_string()),
    ];
    let result = execute_function_args!("examples/rust_funcs/wrappers.rs", "task_a_wrap", arg_vec);

    let res_usize = match result {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_usize, 15);
}

fn test_task_b_cm() {
    let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5)];
    let result = execute_function_args!("examples/rust_funcs/wrappers.rs", "task_b_wrap", arg_vec);

    let res_bool = match result {
        CmTypes::Bool(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_bool, false);
}

fn test_func_call() {

    let name = "dummy";
    call_func(&name, None);

    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
        CmTypes::String("add".to_string()),
    ];
    let name = "task_a";
    let result: CmTypes = call_func(&name, Some(arg_vec));

    let res_usize = match result {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid return type"),
    };

    assert_eq!(res_usize, 15);

    let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5)];
    let name = "task_b";
    let result = call_func(&name, Some(arg_vec));

    let res_bool = match result {
        CmTypes::Bool(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_bool, false);
}

fn main() {
    test_dummy();
    test_task_a_cm();
    test_task_b_cm();
    test_func_call();
    println!("All tests passed!");
}