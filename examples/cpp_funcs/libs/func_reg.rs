use crate::wrappers::*;
use shared::CmTypes;

pub fn call_func(func_name: &str, arg_opt: Option<Vec<CmTypes>>) -> CmTypes {
	match func_name {
		"adder" => {
			let args = arg_opt.unwrap();
			adder_wrap(args)
		},
		_ => panic!("Function not found"),
	}
}
