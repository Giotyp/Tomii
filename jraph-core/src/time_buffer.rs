use crate::utils_rdtsc::*;
use std::collections::HashMap;

pub struct TimeBuffer {
    time_buffers: HashMap<String, Vec<u64>>,
}

impl TimeBuffer {
    pub fn new() -> Self {
        TimeBuffer {
            time_buffers: HashMap::new(),
        }
    }

    pub fn init_task(&mut self, task: &str, runs: usize) {
        // If task with same name exists, ommit
        if self.time_buffers.contains_key(task) {
            return;
        } else {
            self.time_buffers.insert(task.to_string(), vec![0; runs]);
        }
    }

    pub fn add_time(&mut self, task: &str, index: usize, time: u64) {
        if let Some(buffer) = self.time_buffers.get_mut(task) {
            if index >= buffer.len() {
                panic!("Index out of bounds");
            }
            buffer[index] += time;
        } else {
            panic!("Task {} not found in TimeBuffer", task);
        }
    }

    pub fn task_average(&self, task: &str, prec: &str) -> f64 {
        let avg: u64;
        if let Some(buffer) = self.time_buffers.get(task) {
            let runs = buffer.len();
            let sum: u64 = buffer.iter().sum();

            if runs > 0 {
                avg = sum / runs as u64;
            } else {
                avg = 0;
            }
        } else {
            panic!("Task {} not found in TimeBuffer", task);
        }

        match prec {
            "ns" => cycles_to_ns(avg),
            "us" => cycles_to_us(avg),
            "ms" => cycles_to_ms(avg),
            "s" => cycles_to_sec(avg),
            _ => panic!("Invalid precision"),
        }
    }
}
