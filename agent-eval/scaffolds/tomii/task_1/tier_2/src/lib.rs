#![allow(improper_ctypes_definitions)]

use tomii_macro::tomii_export;

#[tomii_export]
pub fn generate_reading(threshold: f64) -> f64 {
    // Calibration load: tie loop to runtime input so LLVM can't constant-fold it away.
    let base = std::hint::black_box(threshold);
    let sink: f64 = (0..100_000u32).map(|i| (base + i as f64).sqrt()).sum();
    std::hint::black_box(sink);
    threshold + 2.5
}

#[tomii_export]
pub fn classify_reading(reading: f64, threshold: f64) -> bool {
    reading > threshold
}

#[tomii_export]
pub fn check_bool(flag: bool) -> bool {
    flag
}

#[tomii_export]
pub fn amplify_reading(reading: f64) -> f64 {
    let base = std::hint::black_box(reading);
    let sink: f64 = (0..100_000u32).map(|i| (base + i as f64).sqrt()).sum();
    std::hint::black_box(sink);
    reading * 2.0
}

#[tomii_export]
pub fn smooth_reading(reading: f64) -> f64 {
    let base = std::hint::black_box(reading);
    let sink: f64 = (0..100_000u32).map(|i| (base + i as f64).sqrt()).sum();
    std::hint::black_box(sink);
    reading * 0.5
}

#[tomii_export]
pub fn compute_sensor_stats() -> f64 {
    42.0
}

#[tomii_export]
pub fn log_stream_event() -> bool {
    true
}

#[tomii_export]
pub fn aggregate_results(stats: f64) -> Vec<f64> {
    vec![stats - 0.5, stats, stats + 0.5]
}

static WRITE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[tomii_export(variadic)]
pub fn write_report(file_path: &str, sensor_summaries: Vec<Vec<f64>>) {
    use std::fs::OpenOptions;
    use std::io::Write;
    let _guard = WRITE_LOCK.lock().unwrap();
    let mut f = OpenOptions::new()
        .create(false)
        .append(true)
        .open(file_path)
        .expect("failed to open result file");
    for (sensor_id, estimates) in sensor_summaries.iter().enumerate() {
        let formatted: Vec<String> = estimates.iter().map(|v| format!("{:.2}", v)).collect();
        writeln!(f, "Sensor-{}: [{}]", sensor_id, formatted.join(", "))
            .expect("failed to write");
    }
}

#[tomii_export]
pub fn get_out_file(env_var: &str, out_file: &str) -> String {
    let dir = std::env::var(env_var)
        .unwrap_or_else(|_| panic!("Environment variable '{}' not set", env_var));
    let path = format!("{}/{}", dir, out_file);
    std::fs::File::create(&path)
        .unwrap_or_else(|_| panic!("Failed to create output file: {}", path));
    path
}

#[tomii_export]
pub fn cleanup_state() {}
