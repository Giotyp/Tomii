pub fn create_buffer(size: usize) -> Vec<usize> {
    let mut buffer = Vec::new();
    for i in 0..size {
        buffer.push(i);
    }
    buffer
}

pub fn create_buffer_wrap(args: Vec<CmTypes>) -> CmTypes {
    let size: usize = match args[0] {
        CmTypes::Usize(size) => size,
        _ => panic!("Invalid argument type"),
    };

    CmTypes::VecUsize(create_buffer(size))
}