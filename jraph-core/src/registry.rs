use crate::cmtypes::*;
use once_cell::sync::OnceCell;

static FUNC_REGISTRY: OnceCell<fn(&str) -> Option<CmPtr>> = OnceCell::new();

pub fn set_func_lookup(f: fn(&str) -> Option<CmPtr>) {
    FUNC_REGISTRY.set(f).unwrap();
}

pub fn get_func(name: &str) -> Option<CmPtr> {
    FUNC_REGISTRY.get().and_then(|f| f(name))
}
