use crate::cmtypes::*;
use crate::func_reg::get_func;
use serde::Deserialize;
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
struct ArgInit {
    #[serde(rename = "type")]
    type_: String,
    value: String,
    mutable: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct InitJson {
    name: String,
    mult_factor: Option<usize>,
    args: Vec<ArgInit>,
    function_name: Option<String>,
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
        let mult_factor = match init.mult_factor {
            Some(mult_factor) => mult_factor,
            None => 1,
        };
        let args_json: &Vec<ArgInit> = &init.args;

        if init.function_name.is_none() {
            // direct variable initialization
            let type_str = args_json[0].type_.clone();
            let value_str = args_json[0].value.clone();

            // Check if type_str is in PARSERS
            let value_cmt = {
                if defined_type(&type_str) {
                    string_to_cmtype(type_str.clone(), value_str.clone(), None).unwrap()
                } else {
                    let is_mut_opt = args_json[0].mutable;
                    string_to_cmtype("Custom".to_string(), value_str.clone(), is_mut_opt).unwrap()
                }
            };

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for _ in 0..mult_factor {
                value_vec.push(value_cmt.clone());
            }
            init_objects.insert(name, value_vec);
        } else {
            // function call needed
            let func_name = init.function_name.as_ref().unwrap().clone();
            let func_ptr = get_func(&func_name).unwrap();

            let mut args: Vec<CmTypes> = Vec::new();
            for arg_json in args_json.iter() {
                let type_str = arg_json.type_.clone();
                let value_str = arg_json.value.clone();

                // check if value_str is in init_objects
                if let Some(init_arg) = init_objects.get(&value_str) {
                    args.push(init_arg[0].clone());
                    continue;
                }

                let type_val = {
                    if defined_type(&type_str) {
                        type_str.clone()
                    } else {
                        "Custom".to_string()
                    }
                };
                let is_mut_opt = arg_json.mutable;

                let arg_cmt = string_to_cmtype(type_val, value_str.clone(), is_mut_opt).unwrap();
                args.push(arg_cmt);
            }

            let value_cmt = func_ptr(args.clone());

            let mut value_vec: Vec<CmTypes> = Vec::new();
            for _ in 0..mult_factor {
                value_vec.push(value_cmt.clone());
            }
            init_objects.insert(name, value_vec);
        }
    }
    Ok(init_objects)
}
