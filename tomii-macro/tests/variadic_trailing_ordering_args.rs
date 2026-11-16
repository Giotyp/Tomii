//! Regression test for the phase-1 `instance_map` validation finding: a
//! `#[tomii_export(variadic)]` kernel whose variadic `$res` argument is
//! followed, on the SAME graph node, by trailing mapped `$barrier`/`$dep`
//! args used purely for ordering. Those always materialize as `CmTypes::None`
//! placeholders in the flattened args slice the runtime hands to the kernel
//! (see `tomii-core::runtime::node_cache::build_arg_cache` and
//! `arg_resolution`'s `$dep`/`$barrier` handling) — never a real value.
//!
//! Before the fix, the generated variadic-collection loop iterated the WHOLE
//! trailing slice unconditionally and panicked on the first `None` it found
//! ("expected numeric CmTypes variant for variadic element"). The fix filters
//! `CmTypes::None` out before collecting, so the kernel sees exactly its own
//! declared values, in order, regardless of what (if anything) follows them
//! on the same node.

use tomii_macro::tomii_export;
use tomii_types::CmTypes;

#[tomii_export(variadic)]
pub fn sum_variadic(values: Vec<f64>) -> f64 {
    values.iter().sum()
}

#[tomii_export(variadic)]
pub fn collect_variadic_strings(labels: Vec<String>) -> String {
    labels.join(",")
}

/// Reproduces the exact panic shape from the phase-1 validation report: a
/// variadic broadcast `$res` arg (several numeric producer instances) followed
/// by trailing mapped `$barrier`/`$dep` args that never carry a value.
#[test]
fn numeric_variadic_ignores_trailing_none_placeholders() {
    let args = vec![
        CmTypes::F64(1.0),
        CmTypes::F64(2.0),
        CmTypes::F64(3.0),
        // Trailing ordering-only slots from mapped `$barrier`/`$dep` args
        // declared after the variadic arg on the same node.
        CmTypes::None,
        CmTypes::None,
    ];
    let result = sum_variadic_cm(&args);
    assert_eq!(result, CmTypes::F64(6.0));
}

/// No trailing `None` at all: the ordinary (pre-existing) shape must still
/// work unchanged.
#[test]
fn numeric_variadic_with_no_trailing_args_still_works() {
    let args = vec![CmTypes::F64(10.0), CmTypes::F64(5.0)];
    let result = sum_variadic_cm(&args);
    assert_eq!(result, CmTypes::F64(15.0));
}

/// Every element trailing-None (a mapped node whose variadic $res list happens
/// to be empty for this instance, e.g. an `instance_map[j]` of length 0 feeding
/// the variadic plus trailing barrier-only args): collects to an empty Vec,
/// not a panic.
#[test]
fn numeric_variadic_all_none_collects_empty() {
    let args = vec![CmTypes::None, CmTypes::None];
    let result = sum_variadic_cm(&args);
    assert_eq!(result, CmTypes::F64(0.0));
}

/// Non-numeric (with_any) variadic path must apply the same filter.
#[test]
fn non_numeric_variadic_ignores_trailing_none_placeholders() {
    let args = vec![
        CmTypes::from_any("a".to_string()),
        CmTypes::from_any("b".to_string()),
        CmTypes::None,
    ];
    let result = collect_variadic_strings_cm(&args);
    match result {
        CmTypes::String(s) => assert_eq!(s.as_ref(), "a,b"),
        other => panic!("unexpected return variant: {other:?}"),
    }
}
