pub fn dummy() {
    println!("This is a dummy function");
}

pub fn task_a(x: usize, y: usize, op: String) -> usize {
    match op.as_str() {
        "add" => x + y,
        "sub" => x - y,
        "mul" => x * y,
        "div" => x / y,
        _ => panic!("Invalid operation"),
    }
}

pub fn task_b(x: usize, lim: usize) -> bool {
    x < lim
}
