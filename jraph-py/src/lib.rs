use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
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