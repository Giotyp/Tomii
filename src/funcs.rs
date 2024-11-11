use shared::CmTypes;

pub fn dummy() {
    println!("This is a dummy function");
}

pub fn task_a (args: Vec<CmTypes>) -> usize {

    let x = match args[0] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type"),
    };
    let y = match args[1] {
        CmTypes::Usize(y) => y.clone(),
        _ => panic!("Invalid argument type"),
    };
    let op = match &args[2] {
        CmTypes::String(op) => op.clone(),
        _ => panic!("Invalid argument type"),
    };

    match op.as_str() {
        "add" => x + y,
        "sub" => x - y,
        "mul" => x * y,
        "div" => x / y,
        _ => panic!("Invalid operation"),
    }
}


pub fn task_b (args: Vec<CmTypes>) -> bool {
    let x = match args[0] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type"),
    };
    let lim = match args[1] {
        CmTypes::Usize(lim) => lim.clone(),
        _ => panic!("Invalid argument type"),
    };

    x < lim
}