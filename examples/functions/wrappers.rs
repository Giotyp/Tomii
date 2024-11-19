use shared::CmTypes;
use crate::simple_funcs::*;

pub fn dummy_wrap() -> () {
	dummy()
}

pub fn task_a_wrap(args: Vec<CmTypes>) -> usize {
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

	task_a(x, y, op)
}

pub fn task_b_wrap(args: Vec<CmTypes>) -> bool {
	let x = match args[0] {
		CmTypes::Usize(x) => x.clone(),
		_ => panic!("Invalid argument type"),
	};

	let lim = match args[1] {
		CmTypes::Usize(lim) => lim.clone(),
		_ => panic!("Invalid argument type"),
	};

	task_b(x, lim)
}
