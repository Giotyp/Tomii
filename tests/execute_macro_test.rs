// Import the library crate where the procedural macro is defined
use cst_macros::*;
use shared::CmTypes;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dummy() {
        execute_function!("examples/functions/simple_funcs.rs", "dummy");
    }

    #[test]
    fn test_task_a_cm() {
        let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5), CmTypes::String("add".to_string())];
        let result = execute_function_args!("examples/functions/simple_funcs.rs", "task_a_cm", arg_vec);
        assert_eq!(result, 15);
    }

    #[test]
    fn test_task_b_cm() {
        let arg_vec = vec![CmTypes::Usize(10), CmTypes::Usize(5)];
        let result = execute_function_args!("examples/functions/simple_funcs.rs", "task_b_cm", arg_vec);
        assert_eq!(result, false);
    }
}