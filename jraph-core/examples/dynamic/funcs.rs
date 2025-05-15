pub fn buf_gen(size: usize) -> Vec<usize> {
    let mut vec = Vec::new();
    for i in 0..size {
        vec.push(i + 1);
    }
    vec
}

pub fn check_condition(buffer: Vec<usize>) -> bool {
    let length = buffer.len();
    if length > 10 {
        return true;
    } else {
        return false;
    }
}

pub fn add_elems(buffer: Vec<usize>) -> usize {
    let mut sum = 0;
    for i in buffer {
        sum += i;
    }
    sum
}

pub fn increase_buf_size(buffer: Vec<usize>, factor: usize) -> usize {
    let length = buffer.len();
    return length + factor;
}
