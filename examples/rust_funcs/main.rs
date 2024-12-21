use shared::CmTypes;
use jraph::func_reg::call_func;

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
    test_func_call();
    println!("All tests passed!");
}