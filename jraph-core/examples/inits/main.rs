use jraph_core::obj_gen::init_objects;
use jraph_core::func_reg::get_func;


fn main() {
    let graph_file = "/home/george/JRaph/jraph-core/examples/graphs/graph.json";

    let obj_map = init_objects(graph_file).unwrap();
    println!("Object map: {:?}\n", obj_map);

    // Call FFT.compute_fft()
    let fft_str = obj_map.get("fft_buf").unwrap();

    let get_buf = get_func("Fft::get_buf").unwrap();
    let compute_fft = get_func("Fft::computefft").unwrap();

    let before_fft = get_buf(vec![fft_str.clone()]);
    println!("Before FFT: {:?}\n", before_fft);

    compute_fft(vec![fft_str.clone()]);

    let after_fft = get_buf(vec![fft_str.clone()]);
    println!("After FFT: {:?}\n", after_fft);
}