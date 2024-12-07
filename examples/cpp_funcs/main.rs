use cst_macros::*;
use shared::CmTypes;

fn test_adder() {
    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
    ];
    let result = execute_function_args!("examples/cpp_funcs/libs/wrappers.rs", "adder_wrap", arg_vec);
    let res_usize = match result {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_usize, 15);
}

fn main() {
    test_adder();
    println!("All tests passed!");
}