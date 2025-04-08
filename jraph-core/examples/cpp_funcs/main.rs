use jraph_core::func_reg::*;
use jraph_core::cmtypes::CmTypes;

fn test_call_func() {
    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
    ];
    let name = "adder";
    let result = call_func(&name, Some(arg_vec));

    let res_usize = match result {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_usize, 15);
}

fn test_get_func() {
    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
    ];
    let name = "adder";
    let adder_f = get_func(&name);
    let result = adder_f(arg_vec);

    let res_usize = match result {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_usize, 15);
}

fn main() {
    test_call_func();
    test_get_func();
    println!("All tests passed!");
}