use std::collections::HashMap;
use std::time::Duration;

pub struct TimeBuffer {
    workers: usize,
    runs: usize,
    // TimeBuffer stores time duration for each task, worker and run
    // time_buffers[s][r][w] = task s: duration vec for run r, worker w
    time_buffers: HashMap<String, Vec<Vec<Vec<Duration>>>>,
    // buffer for total time per run
    time_total: Vec<Duration>,
}

impl TimeBuffer {
    pub fn new(workers: usize, runs: usize) -> Self {
        TimeBuffer {
            workers,
            runs,
            time_buffers: HashMap::new(),
            time_total: vec![Duration::ZERO; runs],
        }
    }

    pub fn init_task(&mut self, task: &str) {
        // If task with same name exists, ommit
        if self.time_buffers.contains_key(task) {
            return;
        } else {
            self.time_buffers.insert(
                task.to_string(),
                vec![vec![Vec::new(); self.workers]; self.runs],
            );
        }
    }

    pub fn add_total_time(&mut self, run_idx: usize, time: Duration) {
        if run_idx >= self.runs {
            panic!("Run Index out of bounds");
        }
        self.time_total[run_idx] += time;
    }

    pub fn add_time(&mut self, task: &str, run_idx: usize, wk_idx: usize, time: Duration) {
        if let Some(buffer) = self.time_buffers.get_mut(task) {
            if run_idx >= self.runs {
                panic!("Run Index out of bounds");
            }
            if wk_idx >= self.workers {
                panic!("Worker Index out of bounds");
            }
            buffer[run_idx][wk_idx].push(time);
        } else {
            panic!("Task {} not found in TimeBuffer", task);
        }
    }

    pub fn print_stats(&self, bench_name: &str, out_file: Option<&str>) {
        let filler = "****************";
        let mut output_buffer: String = format!("Time Statistics for {}\n", bench_name);

        // Add total time
        let total_sum: Duration = self.time_total.iter().sum();
        let total_avg = total_sum / self.runs as u32;
        let total_stat = format!(
            "Average Total Time for {} runs: {:.4?}\n",
            self.runs, total_avg
        );

        output_buffer.push_str(&format!("{}\n{}", filler, total_stat));

        for (task, run_buffer) in self.time_buffers.iter() {
            // Count total tasks and tasks executed by each worker
            // for the last run
            let mut worker_tasks = vec![0; self.workers];
            let mut total_tasks = 0;

            let last_run_buf = &run_buffer[self.runs - 1];
            for i in 0..self.workers {
                let worker_buf = &last_run_buf[i];
                let w_tasks = worker_buf.len();
                total_tasks += w_tasks;
                worker_tasks[i] = w_tasks;
            }

            let active_workers = worker_tasks.iter().filter(|&x| *x > 0).count();

            // Get the sum of all runs for all workers combined
            let run_sum: Duration = run_buffer.iter().flatten().flatten().sum();
            // Average time per run for all workers and for all total tasks
            let run_avg = run_sum / self.runs as u32;
            // Average time per run per worker and for all total tasks
            let run_avg_wk = run_avg / active_workers as u32;
            // Average time per task per run for all workers
            let task_avg = run_avg / total_tasks as u32;
            // Average time per task per run per worker
            let task_avg_wk = task_avg / active_workers as u32;

            let stat_output = &format!(
                "Task {}:\n\
            \tTotal Tasks: {}, Worker Assignments (last run): {:?}\n\
            \tAverage per Run: {:.4?}\n\
            \tAverage per Run, Worker: {:.4?}\n\
            \tAverage per Run, Task: {:.4?}\n\
            \tAverage per Run, Task, Worker: {:.4?}\n",
                task, total_tasks, worker_tasks, run_avg, run_avg_wk, task_avg, task_avg_wk
            );
            output_buffer.push_str(&format!("{}\n{}", filler, stat_output));
        }

        if let Some(out_file) = out_file {
            std::fs::write(out_file, output_buffer).expect("Unable to write file");
        } else {
            println!("{}\n", output_buffer);
        }
    }
}
