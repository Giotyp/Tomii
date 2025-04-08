use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use serde::Deserialize;
use serde_json;
use shared::*;

#[derive(Debug, Deserialize)]
struct InitJson {
    name: String,
    arg_types: Vec<String>,
    args: Vec<String>,
    func: String,
}

#[derive(Debug, Deserialize)]
struct RootJson {
    initializations: Vec<InitJson>,
}

pub fn init_objects(graph_json: &str) -> Result<HashMap<String, CmTypes>, serde_json::Error> {
    let mut file = File::open(graph_json).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // Parse JSON file with defined structure
    let root: RootJson = serde_json::from_str(&contents)?;

    // Create a new Graph
    let mut init_objects: HashMap<String, CmTypes> = HashMap::new();
     
    for init in root.initializations.iter() {
        let name = init.name.clone();

        if init.func.is_empty() {
            // direct variable initialization
            let type_str = init.arg_types[0].clone();
            let arg_str = init.args[0].clone();
            let arg: CmTypes = string_to_primitive(type_str, arg_str).unwrap();
            init_objects.insert(name, arg);
        }
        else {
            // function call needed
            let func_name = init.func.clone();
            // let func_ptr = get_func(&func_name);
            let types_str = init.arg_types.clone();
            let args_str = init.args.clone();
            let args:Vec<CmTypes> = {
                let mut args = Vec::new();
                for (type_str, arg_str) in types_str.iter().zip(args_str.iter()) {

                    // check if arg_str is in init_objects
                    if let Some(arg) = init_objects.get(arg_str) {
                        args.push(arg.clone());
                        continue;
                    }

                    args.push(string_to_primitive(type_str.clone(), arg_str.clone()).unwrap());
                }
                args
            };
            let value: CmTypes = cmtype_object(func_name, args);
            init_objects.insert(name, value);
        }
    }
    Ok(init_objects)
}