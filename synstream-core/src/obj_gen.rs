use crate::debug::print_debug;
use crate::func_reg::get_func;
use crate::json_structs::*;
use crate::prelude::*;
use rapidhash::{HashMapExt, RapidHashMap};
use serde_json;
use std::sync::atomic::Ordering::SeqCst;
use synstream_types::*;

pub fn init_objects(
    initializations_json: &Vec<InitJson>,
    workers: usize,
) -> Result<(Vec<Vec<CmTypes>>, RapidHashMap<String, usize>), serde_json::Error> {
    // Create a new RapidHashMap to store the initialized objects
    let mut init_objects: Vec<Vec<CmTypes>> = Vec::new();
    let mut obj_id_map: RapidHashMap<String, usize> = RapidHashMap::new();

    // Keep index 0 for $index, 1 for $workers, 2 for $network -- resolved at runtime
    let _ = ObjectCount.fetch_add(2, SeqCst);
    obj_id_map.insert("$index".to_string(), 0);
    obj_id_map.insert("$workers".to_string(), 1);
    // Initialize $index and $workers
    init_objects.push(vec![CmTypes::Usize(0)]);
    init_objects.push(vec![CmTypes::Usize(1)]);

    for init in initializations_json.iter() {
        let name = &init.name;
        print_debug(|| format!("Initializing object: {}", name));
        let factor = match &init.factor {
            Some(factor) => factor.search(&init_objects, &obj_id_map, workers),
            None => 1,
        };
        let args_json: &Vec<ArgInit> = &init.args;

        if init.function_name.is_none() {
            // direct variable initialization
            let type_str = &args_json[0].type_;
            let value_str = &args_json[0].value;

            // Check if type_str is in PARSERS
            let value_cmt_res = string_to_cmtype(type_str.to_string(), value_str.to_string());
            let value_cmt = match value_cmt_res {
                Ok(cmt) => cmt,
                Err(e) => {
                    eprintln!(
                        "Error parsing type '{}' with value '{}': {}",
                        type_str, value_str, e
                    );
                    panic!("Create an init function to handle this type.");
                }
            };

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for _ in 0..factor {
                value_vec.push(value_cmt.clone());
            }
            // fetch-add ObjectCount
            let obj_id = ObjectCount.fetch_add(1, SeqCst);
            obj_id_map.insert(name.clone(), obj_id as usize);
            init_objects.push(value_vec);
        } else {
            // function call needed
            let func_name = init.function_name.as_ref().unwrap();
            let func_ptr = get_func(func_name).unwrap();

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for i in 0..factor {
                let mut args: Vec<CmTypes> = Vec::new();
                for arg_json in args_json.iter() {
                    let type_str = &arg_json.type_;
                    let value_str = &arg_json.value;

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
                    if let Some(obj_id) = obj_id_map.get(value_str.as_str()) {
                        let init_arg = &init_objects[*obj_id];
                        args.push(init_arg[0].clone());
                        continue;
                    }

                    let arg_cmt_res = string_to_cmtype(type_str.to_string(), value_str.to_string());
                    let arg_cmt = match arg_cmt_res {
                        Ok(cmt) => cmt,
                        Err(e) => {
                            eprintln!(
                                "Error parsing type '{}' with value '{}': {}",
                                type_str, value_str, e
                            );
                            panic!("Create an init function to handle this type.");
                        }
                    };
                    args.push(arg_cmt);
                }

                let value_cmt = func_ptr(args.clone());
                value_vec.push(value_cmt.clone());
            }
            // fetch-add ObjectCount
            let obj_id = ObjectCount.fetch_add(1, SeqCst);
            obj_id_map.insert(name.clone(), obj_id as usize);
            init_objects.push(value_vec);
        }
    }
    Ok((init_objects, obj_id_map))
}
