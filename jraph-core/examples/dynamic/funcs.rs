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

pub fn multiply_elems(buffer: Vec<usize>) -> usize {
    let mut product = 1;
    for i in buffer {
        product *= i;
    }
    product
}
