# tomii-macro

Procedural macros for wrapping plugin functions for the
[Tomii](https://github.com/Giotyp/Tomii) task-graph runtime.

This crate provides the `#[tomii_export]` attribute macro, which generates the
type-erased wrapper and registry glue needed to expose a plain Rust function as a
Tomii plugin node. Using the macro removes the need to hand-write wrapper
functions against the `tomii-types` ABI.

```rust
use tomii_macro::tomii_export;

#[tomii_export]
fn add(a: f64, b: f64) -> f64 {
    a + b
}
```

See the [Tomii repository](https://github.com/Giotyp/Tomii) for full plugin
authoring documentation.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/Giotyp/Tomii/blob/main/LICENSE).
