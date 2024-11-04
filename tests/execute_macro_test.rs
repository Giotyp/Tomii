// Import the library crate where the procedural macro is defined
use cst_macros::execute_function;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dummy() {
        execute_function!("src/funcs.rs", "dummy");
    }
}