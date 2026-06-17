# tomii-converter

Build-time tool that converts Rust/C plugin headers into
[Tomii](https://github.com/Giotyp/Tomii) wrapper and registry files.

`tomii-converter` parses a plugin source or header file (Rust `.rs` or C
`.h`/`.hpp`) and emits the wrapper functions and function registry that
`tomii-core` includes at build time. It is driven by the `FUNC_PATH`,
`WRAP_PATH`, and `REG_PATH` environment variables and is normally invoked from
`tomii-core`'s build script, but it also ships a standalone `tomii-converter`
binary for manual generation.

```bash
cargo install tomii-converter
tomii-converter --help
```

See the [Tomii repository](https://github.com/Giotyp/Tomii) for details on the
plugin build pipeline.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/Giotyp/Tomii/blob/main/LICENSE).
