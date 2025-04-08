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
    comp::rayon_gemm();
    println!("");
    multithread::multi_sleep();
    println!("");
    multithread::task_spawn();
}
