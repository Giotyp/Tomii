// Import the library crate where the procedural macro is defined
use cst_macros::*;
use shared::CmTypes;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dummy() {
        execute_function!("src/funcs.rs", "dummy");
    }

    #[test]
    fn test_task_a() {
        let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5), CmTypes::String("add".to_string())];
        let result = execute_function_args!("src/funcs.rs", "task_a", arg_vec);
        assert_eq!(result, 15);
    }
}