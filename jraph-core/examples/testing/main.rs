#![allow(unused)]
mod comp;
mod functions;
mod retrieve;

fn main() {
    // retrieve::retrieve();
    comp::vec_mat();
    println!("");
    comp::mt_cgemm();
}
