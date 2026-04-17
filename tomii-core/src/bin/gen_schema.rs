/// Emit the JSON Schema for GraphFile to stdout.
///
/// Usage (from workspace root):
///   WRAP_PATH=/dev/null REG_PATH=/dev/null \
///     cargo run -p tomii-core --bin gen-schema > tomii/schema.json
use tomii_core::json_structs::GraphFile;

fn main() {
    let schema = schemars::schema_for!(GraphFile);
    println!(
        "{}",
        serde_json::to_string_pretty(&schema).expect("schema serialization failed")
    );
}
