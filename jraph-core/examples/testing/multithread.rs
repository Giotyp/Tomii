use std::time::Instant;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use core_affinity;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::cell::RefCell;

thread_local! {
    static THREAD_TIMER: RefCell<Instant> = RefCell::new(Instant::now());
}

pub fn multi_sleep() {
    let core_offset = 1;
    let workers = 3;
    let mut core_ids = core_affinity::get_core_ids().unwrap();
    core_ids.sort();
    let cores_to_use: Vec<core_affinity::CoreId> =
        core_ids[core_offset..core_offset + workers].to_vec();
    let threadpool = ThreadPoolBuilder::new()
        .num_threads(workers)
        .start_handler(move |thread_index| {
            // Pin each thread to a specific core
            let core_id = cores_to_use[thread_index];
            core_affinity::set_for_current(core_id);
        })
        .build()
        .unwrap();

    println!("Using {} workers", workers);

    let start_total = Instant::now();
    threadpool.install(|| {
        (0..workers).into_par_iter().for_each(|_| {
            THREAD_TIMER.with(|timer| {
                let start = timer.replace(Instant::now());
                std::thread::sleep(std::time::Duration::from_secs(1));
                let duration = start.elapsed();
                let idx = rayon::current_thread_index().unwrap();
                println!("Thread {:?} slept for: {:.4?}", idx, duration);
            });
        });
    });

    let end_total = start_total.elapsed();
    println!("Total time: {:.4?}", end_total);
}

pub fn task_spawn() {
    let core_offset = 1;
    let workers = 2;
    let mut core_ids = core_affinity::get_core_ids().unwrap();
    core_ids.sort();
    let cores_to_use: Vec<core_affinity::CoreId> =
        core_ids[core_offset..core_offset + workers].to_vec();
    let threadpool = ThreadPoolBuilder::new()
        .num_threads(workers)
        .start_handler(move |thread_index| {
            // Pin each thread to a specific core
            let core_id = cores_to_use[thread_index];
            core_affinity::set_for_current(core_id);
        })
        .build()
        .unwrap();

    println!("Using {} workers", workers);
    let task_counter = Arc::new(AtomicUsize::new(0));
    task_counter.fetch_add(1, Ordering::SeqCst);
    let task_counter_clone = task_counter.clone();

    threadpool.spawn(move || {
        let idx = rayon::current_thread_index().unwrap();
        println!("Thread {:?} sleeping for 3 seconds", idx);
        std::thread::sleep(std::time::Duration::from_secs(5));
        println!("Thread {:?} woke up", idx);
        task_counter_clone.fetch_sub(1, Ordering::SeqCst);
    });

    let task_counter_clone = task_counter.clone();
    threadpool.spawn(move || {
        let idx = rayon::current_thread_index().unwrap();
        println!("Thread {:?} sleeping for 3 seconds", idx);
        std::thread::sleep(std::time::Duration::from_secs(5));
        println!("Thread {:?} woke up", idx);
        task_counter_clone.fetch_sub(1, Ordering::SeqCst);
    });

    println!("Main thread sleeping for 2 seconds");
    std::thread::sleep(std::time::Duration::from_secs(2));
    while task_counter.load(Ordering::SeqCst) > 0 {
        std::thread::sleep(Duration::from_micros(1));
    }
}