use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use serde::Deserialize;
use serde_json;
use crate::cmtypes::*;
use crate::func_reg::get_func;

#[derive(Debug, Deserialize)]
struct InitJson {
    name: String,
    arg_types: Vec<String>,
    args: Vec<String>,
    func: String,
    mult_factor: usize,
}

#[derive(Debug, Deserialize)]
struct RootJson {
    initializations: Vec<InitJson>,
}

pub fn init_objects(graph_json: &str) -> Result<HashMap<String, Vec<CmTypes>>, serde_json::Error> {
    let mut file = File::open(graph_json).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // Parse JSON file to look for initializations
    let root: RootJson = serde_json::from_str(&contents)?;

    // Create a new HashMap to store the initialized objects
    let mut init_objects: HashMap<String, Vec<CmTypes>> = HashMap::new();
     
    for init in root.initializations.iter() {
        let name = init.name.clone();

        if init.func.is_empty() {
            // direct variable initialization
            let type_str = init.arg_types[0].clone();
            let arg_str = init.args[0].clone();
            let mult_factor = init.mult_factor;

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for _ in 0..mult_factor {
                let arg: CmTypes = string_to_cmtype(type_str.clone(), arg_str.clone()).unwrap();
                value_vec.push(arg);
            }
            init_objects.insert(name, value_vec);
        }
        else {
            // function call needed
            let func_name = init.func.clone();
            let func_ptr = get_func(&func_name).unwrap();
            let types_str = init.arg_types.clone();
            let args_str = init.args.clone();
            let args:Vec<CmTypes> = {
                let mut args = Vec::new();
                for (type_str, arg_str) in types_str.iter().zip(args_str.iter()) {

                    // check if arg_str is in init_objects
                    if let Some(arg) = init_objects.get(arg_str) {
                        args.push(arg[0].clone());
                        continue;
                    }

                    args.push(string_to_cmtype(type_str.clone(), arg_str.clone()).unwrap());
                }
                args
            };
            let mult_factor = init.mult_factor;

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for _ in 0..mult_factor {
                let value: CmTypes = func_ptr(args.clone());
                value_vec.push(value);
            }
            init_objects.insert(name, value_vec);
        }
    }
    Ok(init_objects)
}