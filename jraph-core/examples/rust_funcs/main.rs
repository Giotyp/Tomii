use shared::CmTypes;
use jraph_core::func_reg::*;

fn test_call_func() {

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

fn test_get_func() {

    let name = "dummy";
    let dummy_f = get_func(&name);
    dummy_f(vec![CmTypes::None()]);

    let arg_vec = vec![
        CmTypes::Usize(10),
        CmTypes::Usize(5),
        CmTypes::String("add".to_string()),
    ];
    let name = "task_a";
    let taska_f = get_func(&name);
    let result = taska_f(arg_vec);

    let res_usize = match result {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid return type"),
    };

    assert_eq!(res_usize, 15);

    let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5)];
    let name = "task_b";
    let taskb_f = get_func(&name);
    let result = taskb_f(arg_vec);

    let res_bool = match result {
        CmTypes::Bool(x) => x,
        _ => panic!("Invalid return type"),
    };
    assert_eq!(res_bool, false);
}

fn main() {
    test_call_func();
    test_get_func();
    println!("All tests passed!");
}