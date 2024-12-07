use shared::CmTypes;

#[link(name = "adder")]
extern "C" {
	fn adder(
		a: usize,
		b: usize,
	) -> usize;
}

pub fn adder_wrap(args: Vec<CmTypes>) -> CmTypes {
	let a = match args[0] {
		CmTypes::Usize(a) => a.clone(),
		_ => panic!("Invalid argument type"),
	};

	let b = match args[1] {
		CmTypes::Usize(b) => b.clone(),
		_ => panic!("Invalid argument type"),
	};

	CmTypes::Usize(unsafe{adder(a, b)})
}
