SCRIPTS_DIR=$(dirname "$(readlink -f "$0")")
MIMOLIB_DIR=$(readlink -f "$SCRIPTS_DIR/..")

export FUNC_PATH="$MIMOLIB_DIR/src/lib.rs"
export WRAP_PATH="$MIMOLIB_DIR/wrap/wrappers_ptr.rs"
export REG_PATH="$MIMOLIB_DIR/wrap/reg.rs"