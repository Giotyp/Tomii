use crate::simple_funcs::*;

use shared::CmTypes;
pub fn dummy_wrap() -> CmTypes {
	dummy();
	CmTypes::None()
}

pub fn task_a_wrap(args: Vec<CmTypes>) -> CmTypes {
	let x = match args[0] {
		CmTypes::Usize(x) => x.clone(),
		_ => panic!("Invalid argument type"),
	};

	let y = match args[1] {
		CmTypes::Usize(y) => y.clone(),
		_ => panic!("Invalid argument type"),
	};

	let op = match &args[2] {
		CmTypes::String(op) => op.clone(),
		_ => panic!("Invalid argument type"),
	};

	CmTypes::Usize(task_a(x, y, op))
}

pub fn task_b_wrap(args: Vec<CmTypes>) -> CmTypes {
	let x = match args[0] {
		CmTypes::Usize(x) => x.clone(),
		_ => panic!("Invalid argument type"),
	};

	let lim = match args[1] {
		CmTypes::Usize(lim) => lim.clone(),
		_ => panic!("Invalid argument type"),
	};

	CmTypes::Bool(task_b(x, lim))
}
