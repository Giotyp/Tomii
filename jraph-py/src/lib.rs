use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::collections::HashMap;
use jraph_core::graph_struct::*;
use jraph_core::graph_gen::from_json as rust_from_json;

#[pyfunction]
fn from_json(graph_json: &str) -> PyResult<PyGraph> {
    let graph = rust_from_json(graph_json).unwrap();
    let pygraph = PyGraph{graph};
    Ok(pygraph)
}

#[pyclass]
struct PyGraph {
    graph: Graph,
}

#[pymethods]
impl PyGraph {

    fn len(&self) -> usize {
        self.graph.len()
    }

    fn get_nodes(&self, stage_no: usize) -> &Vec<String> {
        self.graph.stage(stage_no).node_names()
    }

    fn get_arg_types(&self, stage_no: usize, node_name: &str) -> Vec<String> {
        let node = self.graph.stage(stage_no).node(node_name).unwrap();
        let node = node.read().unwrap();
        let task = node.task();
        let args_enum = task.args();
        let args_vec: Vec<String> = args_enum.iter().map(|x| x.arg_name()).collect();
        args_vec
    }

    fn node_info(&self, stage_no: usize, node_name: &str) -> HashMap<String, String> {
        let node = self.graph.stage(stage_no).node(node_name).unwrap();
        let node = node.read().unwrap();
        let task = node.task();

        let mult_factor = node.mult_factor();
        let succ_index = node.successors_index();
        let succ_names = node.successors_names();
        let function_name = task.function_name();
        let function_path = task.function_path();
        let args_enum = task.args();
        let args_vec: Vec<String> = args_enum.iter().map(|x| x.to_string()).collect();

        let info = HashMap::from([
            ("mult_factor".to_string(), mult_factor.to_string()),
            ("function_path".to_string(), function_path.clone()),
            ("function_name".to_string(), function_name.clone()),
            ("successors_index".to_string(), succ_index.join(", ")),
            ("successors_names".to_string(), succ_names.join(", ")),
            ("args".to_string(), args_vec.join(", "))
        ]);
        info
    }

    fn get_func(&self, stage_no: usize, node_name: &str) -> String {
        let node = self.graph.stage(stage_no).node(node_name).unwrap();
        node.read().unwrap().task().function_name().clone()
    }

    // fn execute(&self, stage_no: usize, node_name: &str){
    //     let node = self.graph.stage(stage_no).node(node_name).unwrap().read().unwrap();
    //     let task = node.task();
    //     let arg_vec = task.args().clone();
    //     let name = task.function_name().clone();
    //     // let result = call_func(&name, Some(arg_vec));
    //     println!("{:?}", result);
    // }

    fn generate_dot(&self) -> String {
        self.graph.generate_dot()
    }

    fn print_graph(&self) {
        self.graph.print_graph()
    }
}

#[pymodule]
fn pyjraph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(from_json, m)?)?;
    m.add_class::<PyGraph>()?;
    Ok(())
}