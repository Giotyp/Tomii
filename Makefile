.PHONY: schema test lint fmt fmt-check wheel wheel-sdist

# Temp stubs used only during schema generation — not baked into any build
_SCHEMA_WRAP := /tmp/_ss_schema_wrap.rs
_SCHEMA_REG  := /tmp/_ss_schema_reg.rs

## schema: Regenerate tomii/_generated.py from tomii-core/src/json_structs.rs
##         Run this after changing any struct in json_structs.rs.
schema:
	@printf '// schema-gen stub\n' > $(_SCHEMA_WRAP)
	@printf 'use tomii_types::CmPtr;\npub fn get_func(_name: &str) -> Option<CmPtr> { None }\n' \
		> $(_SCHEMA_REG)
	WRAP_PATH=$(_SCHEMA_WRAP) REG_PATH=$(_SCHEMA_REG) \
		cargo run -p tomii-core --bin gen-schema > tomii/schema.json
	datamodel-codegen \
		--input tomii/schema.json \
		--input-file-type jsonschema \
		--output tomii/_generated.py \
		--output-model-type pydantic_v2.BaseModel \
		--allow-population-by-field-name \
		--use-field-description
	@echo "[tomii] schema regenerated — commit schema.json and _generated.py"

## test: Run the Python test suite
test:
	python -m pytest tomii/tests/ -v

## fmt: Format all Python and Rust source in-place
fmt:
	ruff format tomii/
	cargo fmt

## fmt-check: Check formatting without modifying files (used in CI)
fmt-check:
	ruff format --check tomii/
	cargo fmt --check

## lint: Check formatting and type-check the Python package
lint:
	ruff format --check tomii/
	python -m mypy tomii/

## wheel: Build a release wheel for the current interpreter.
##        Copies the tomii-core binary into tomii/_bin/ first, then
##        invokes maturin to compile the bridge and assemble the wheel.
wheel:
	cargo build --release --features embed-python -p tomii-core --bin main
	cp target/release/main tomii/_bin/main
	maturin build --release

## wheel-sdist: Build a source distribution.
wheel-sdist:
	maturin sdist
