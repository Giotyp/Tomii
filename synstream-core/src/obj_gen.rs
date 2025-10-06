use crate::debug::print_debug;
use crate::func_reg::get_func;
use crate::json_structs::*;
use rapidhash::{HashMapExt, RapidHashMap};
use serde_json;
use synstream_types::*;

pub fn init_objects(
    initializations_json: &Vec<InitJson>,
    workers: usize,
) -> Result<RapidHashMap<String, Vec<CmTypes>>, serde_json::Error> {
    // Create a new RapidHashMap to store the initialized objects
    let mut init_objects: RapidHashMap<String, Vec<CmTypes>> = RapidHashMap::new();

    for init in initializations_json.iter() {
        let name = init.name.clone();
        print_debug(&format!("Initializing object: {}", name));
        let factor = match &init.factor {
            Some(factor) => factor.search(&init_objects, workers),
            None => 1,
        };
        let args_json: &Vec<ArgInit> = &init.args;

        if init.function_name.is_none() {
            // direct variable initialization
            let type_str = args_json[0].type_.clone();
            let value_str = args_json[0].value.clone();

            // Check if type_str is in PARSERS
            let value_cmt_res = string_to_cmtype(type_str.clone(), value_str.clone());
            let value_cmt = match value_cmt_res {
                Ok(cmt) => cmt,
                Err(e) => {
                    eprintln!("Error parsing type '{}': {}", type_str, e);
                    panic!("Create an init function to handle this type.");
                }
            };

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for _ in 0..factor {
                value_vec.push(value_cmt.clone());
            }
            init_objects.insert(name.clone(), value_vec);
        } else {
            // function call needed
            let func_name = init.function_name.as_ref().unwrap().clone();
            let func_ptr = get_func(&func_name).unwrap();

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for i in 0..factor {
                let mut args: Vec<CmTypes> = Vec::new();
                for arg_json in args_json.iter() {
                    let type_str = arg_json.type_.clone();
                    let value_str = arg_json.value.clone();

                    if value_str == "$workers" {
                        // special case for workers
                        args.push(CmTypes::Usize(workers));
                        continue;
                    }

                    if value_str == "$index" {
                        // special case for index
                        args.push(CmTypes::Usize(i));
                        continue;
                    }

                    // check if value_str is in init_objects
                    if let Some(init_arg) = init_objects.get(&value_str) {
                        args.push(init_arg[0].clone());
                        continue;
                    }

                    let arg_cmt_res = string_to_cmtype(type_str.clone(), value_str.clone());
                    let arg_cmt = match arg_cmt_res {
                        Ok(cmt) => cmt,
                        Err(e) => {
                            eprintln!("Error parsing type '{}': {}", type_str, e);
                            panic!("Create an init function to handle this type.");
                        }
                    };
                    args.push(arg_cmt);
                }

                let value_cmt = func_ptr(args.clone());
                value_vec.push(value_cmt.clone());
            }
            init_objects.insert(name.clone(), value_vec);
        }
    }
    Ok(init_objects)
}
