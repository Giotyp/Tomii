#![allow(unused)]
mod comp;
mod functions;
mod multithread;
mod retrieve;

fn main() {
    retrieve::retrieve();
    comp::vec_mat();
    println!("");
    comp::mt_cgemm();
    println!("");
    multithread::multi_sleep();
    println!("");
}
