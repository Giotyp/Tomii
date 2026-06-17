# tomii-types

Type-erased value types shared across [Tomii](https://github.com/Giotyp/Tomii)
plugins and runtime.

This crate defines `CmTypes`, the enum used to pass type-erased values across
dynamically-loaded plugin boundaries, along with the supporting pointer and
handle types. It is a foundational dependency of
[`tomii-core`](https://crates.io/crates/tomii-core) and of plugins built for the
Tomii task-graph framework.

Most users do not depend on this crate directly — it is pulled in transitively by
`tomii-core` and by the `tomii-macro` plugin wrappers.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/Giotyp/Tomii/blob/main/LICENSE).
