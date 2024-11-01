pub fn task_a (x: usize, y:usize, op: &str) -> usize {
    match op {
        "add" => x + y,
        "sub" => x - y,
        "mul" => x * y,
        "div" => x / y,
        _ => panic!("Invalid operation"),
    }
}


pub fn task_b (x: usize, lim: usize) -> bool {
    x < lim
}