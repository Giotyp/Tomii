use crate::wrappers::*;
use shared::CmTypes;

pub fn call_func(func_name: &str, arg_opt: Option<Vec<CmTypes>>) -> CmTypes {
	match func_name {
		"dummy" => {
			dummy_wrap()
		},
		"task_a" => {
			let args = arg_opt.unwrap();
			task_a_wrap(args)
		},
		"task_b" => {
			let args = arg_opt.unwrap();
			task_b_wrap(args)
		},
		_ => panic!("Function not found"),
	}
}
