use std::sync::OnceLock;

static DEBUG: OnceLock<bool> = OnceLock::new();

pub fn init_debug(debug: bool) {
    DEBUG.set(debug).expect("Failed to set DEBUG variable");
}

#[inline(always)]
pub fn print_debug(msg: impl FnOnce() -> String) {
    if *DEBUG.get().unwrap_or(&false) {
        println!("DB: {}", msg());
    }
}

pub fn is_debug_enabled() -> bool {
    *DEBUG.get().unwrap_or(&false)
}
