"""Generate Python synstream API code from a GraphFile-compatible dict."""

from __future__ import annotations

import textwrap
from typing import Any, Optional


def generate_python(graph_data: dict) -> str:
    """Produce a build_graph() function string from a GraphFile dict.

    Parameters
    ----------
    graph_data:
        A dict matching the GraphFile JSON schema (same format as SynStream JSON files).

    Returns
    -------
    str
        Valid Python source that imports synstream and defines ``build_graph() -> ss.Graph``.
    """
    lines: list[str] = []
    lines.append("import synstream as ss")
    lines.append("from synstream import Condition, Loop")
    lines.append("")
    lines.append("")
    lines.append("def build_graph() -> ss.Graph:")
    lines.append("    app = ss.Graph()")
    lines.append("")

    inits = graph_data.get("initializations", [])
    nodes = graph_data.get("nodes", [])
    post_nodes = graph_data.get("post_nodes") or []

    # Collect var names for forward references in factor/args
    var_names: set[str] = {iv["name"] for iv in inits}

    # --- Variables ---
    if inits:
        lines.append("    # Variables")
        for iv in inits:
            name = iv["name"]
            factor = iv.get("factor")
            func = iv.get("function")
            args_raw = iv.get("args", [])

            kwargs: list[str] = []

            if func:
                kwargs.append(f'func="{func}"')
                if args_raw:
                    encoded = _encode_init_args(args_raw, var_names)
                    kwargs.append(f"args=[{encoded}]")
            else:
                # Direct value: first arg's value
                if args_raw:
                    val = _parse_init_value(args_raw[0])
                    kwargs.insert(0, val)  # positional

            if factor is not None:
                kwargs.append(f"factor={_factor_expr(factor, var_names)}")

            if func:
                lines.append(f"    {_pyname(name)} = app.var({name!r}, {', '.join(kwargs)})")
            else:
                # positional value is first, rest are kwargs
                val_arg = kwargs[0] if kwargs else "None"
                rest = kwargs[1:]
                rest_str = (", " + ", ".join(rest)) if rest else ""
                lines.append(f"    {_pyname(name)} = app.var({name!r}, {val_arg}{rest_str})")
        lines.append("")

    # Build node name → Python identifier map
    node_pynames: dict[str, str] = {iv["name"]: _pyname(iv["name"]) for iv in inits}
    for n in nodes + post_nodes:
        node_pynames[n["name"]] = _pyname(n["name"])

    # --- Compute nodes ---
    if nodes:
        lines.append("    # Nodes")
        for n in nodes:
            _append_node(lines, n, node_pynames, var_names, is_post=False)
        lines.append("")

    # --- Post-nodes ---
    if post_nodes:
        lines.append("    # Post-nodes")
        for n in post_nodes:
            _append_node(lines, n, node_pynames, var_names, is_post=True)
        lines.append("")

    lines.append("    return app")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pyname(name: str) -> str:
    """Convert a graph name to a safe Python identifier (replace hyphens)."""
    return name.replace("-", "_")


def _factor_expr(factor: Any, var_names: set[str]) -> str:
    """Render a factor value as a Python expression."""
    if isinstance(factor, int):
        return str(factor)
    if isinstance(factor, str):
        if factor in var_names:
            return _pyname(factor)
        # Numeric string
        try:
            int(factor)
            return factor
        except ValueError:
            return _pyname(factor)
    return str(factor)


def _parse_init_value(arg: dict) -> str:
    """Convert an ArgInit dict to a Python literal expression."""
    type_ = arg.get("type", "usize")
    value = arg.get("value", "")

    if type_ == "$ref":
        return _pyname(value)
    if type_ == "bool":
        return "True" if value == "true" else "False"
    if type_ in ("f32", "f64"):
        return f"ss.{type_}({value})"
    if type_.startswith("i") or type_.startswith("u"):
        # Integer types: plain int unless it needs a wrapper
        if type_ == "usize":
            return value  # plain int, infer_type handles it
        return f"ss.{type_}({value})"
    if type_ == "String":
        return f"ss.String({value!r})"
    if type_.startswith("Complex"):
        return f"ss.{type_}({value})"
    if type_.startswith("Vec"):
        return f"ss.Vec({type_[4:-1]!r}, [{value}])"
    # Fallback: plain value
    return repr(value)


def _encode_init_args(args: list[dict], var_names: set[str]) -> str:
    """Encode a list of ArgInit dicts as a comma-separated Python expression string."""
    parts: list[str] = []
    for a in args:
        type_ = a.get("type", "")
        value = a.get("value", "")
        if type_ == "$ref":
            parts.append(_pyname(value))
        else:
            parts.append(_parse_init_value(a))
    return ", ".join(parts)


def _encode_arg(arg: dict, node_pynames: dict[str, str], var_names: set[str]) -> Optional[str]:
    """Encode a single ArgJson dict as a Python expression. Returns None to skip."""
    type_ = arg.get("type", "")
    value = arg.get("value")
    pred = arg.get("predecessor")

    if type_ == "$ref":
        return _pyname(value) if value else None

    if type_ in ("$res", "$dep", "$barrier") and pred:
        src_name = pred.get("name", "")
        indexes = pred.get("indexes", "0")
        group_by = pred.get("group_by")
        src_py = node_pynames.get(src_name, _pyname(src_name))

        method = {"$res": "out", "$dep": "dep", "$barrier": "wait"}[type_]
        index_args = _format_indexes(indexes, var_names)
        gb_kw = f", group_by={_factor_expr(group_by, var_names)}" if group_by is not None else ""
        return f"{src_py}.{method}({index_args}{gb_kw})"

    # Literal type
    if type_ == "bool":
        return "True" if value == "true" else "False"
    if type_ in ("f32", "f64"):
        return f"ss.{type_}({value})"
    if type_ == "usize":
        return str(value)  # plain int
    if type_.startswith("i") or type_.startswith("u"):
        return f"ss.{type_}({value})"
    if type_ == "String":
        return f"ss.String({value!r})"
    if type_.startswith("Complex"):
        return f"ss.{type_}({value})"
    if type_.startswith("Vec"):
        inner = type_[4:-1]
        return f"ss.Vec({inner!r}, [{value}])"

    # Unknown — skip
    return None


def _format_indexes(indexes: str, var_names: set[str]) -> str:
    """Convert an indexes string to Python arguments for out()/dep()/wait()."""
    if "," in indexes:
        # Comma-separated list → pass as a list
        return f"[{indexes}]"
    if "-" in indexes:
        parts = indexes.split("-", 1)
        start = parts[0].strip()
        end = parts[1].strip()
        end_expr = _pyname(end) if end in var_names else end
        return f"{start}, {end_expr}"
    # Single index
    return indexes.strip()


def _encode_condition(cond: dict, node_pynames: dict[str, str], var_names: set[str]) -> str:
    """Encode a NodeConditionJson dict as a Condition(...) expression."""
    op = cond.get("operation", "Eq")
    val = cond.get("value", "")
    val_type = cond.get("value_type", "bool")
    func = cond.get("function", "")
    args_raw = cond.get("args", [])

    # Render value — bool needs to stay as Python bool (not a string)
    if val_type == "bool":
        val_repr = "True" if val == "true" else "False"
    elif val_type in ("f32", "f64"):
        val_repr = repr(float(val)) if val else repr(val)
    elif val_type in ("usize", "isize") or val_type[0] in ("i", "u"):
        val_repr = val  # plain integer literal
    else:
        val_repr = repr(val)

    args_exprs = [e for a in args_raw if (e := _encode_arg(a, node_pynames, var_names)) is not None]
    args_str = f"[{', '.join(args_exprs)}]" if args_exprs else "[]"

    return (
        f"Condition(operation={op!r}, value={val_repr}, "
        f"value_type={val_type!r}, func={func!r}, args={args_str})"
    )


def _encode_loop(loop: dict, var_names: set[str]) -> str:
    """Encode a LoopJson dict as a Loop(...) expression."""
    name = loop.get("name", "")
    factor = loop.get("factor")
    factor_expr = _factor_expr(factor, var_names) if factor is not None else "1"
    return f'Loop(name={name!r}, factor={factor_expr})'


def _append_node(
    lines: list[str],
    n: dict,
    node_pynames: dict[str, str],
    var_names: set[str],
    is_post: bool,
) -> None:
    """Append the app.node(...) or app.post_node(...) call for a node."""
    name = n["name"]
    func = n["function"]
    factor = n.get("factor")
    priority = n.get("priority")
    use_workers = n.get("use_workers")
    group_size = n.get("group_size")
    loop = n.get("loop")
    loop_args_raw = n.get("loop_args")
    cond = n.get("condition")
    args_raw = n.get("args", [])
    py_name = node_pynames.get(name, _pyname(name))
    method = "post_node" if is_post else "node"

    kwargs: list[str] = [f'func="{func}"']

    if factor is not None:
        kwargs.append(f"factor={_factor_expr(factor, var_names)}")

    if priority:
        kwargs.append(f"priority={priority!r}")

    if group_size is not None:
        kwargs.append(f"group_size={_factor_expr(group_size, var_names)}")

    if use_workers:
        kwargs.append(f"use_workers={use_workers!r}")

    if loop:
        kwargs.append(f"loop={_encode_loop(loop, var_names)}")

    if loop_args_raw:
        loop_args_exprs = [
            e for a in loop_args_raw
            if (e := _encode_arg(a, node_pynames, var_names)) is not None
        ]
        kwargs.append(f"loop_args=[{', '.join(loop_args_exprs)}]")

    if cond:
        kwargs.append(f"condition={_encode_condition(cond, node_pynames, var_names)}")

    args_exprs = [
        e for a in args_raw
        if (e := _encode_arg(a, node_pynames, var_names)) is not None
    ]
    if args_exprs:
        kwargs.append(f"args=[{', '.join(args_exprs)}]")

    # Build the call, wrapping long lines at 88 chars
    call = f"    {py_name} = app.{method}({name!r}, {', '.join(kwargs)})"
    if len(call) <= 88:
        lines.append(call)
    else:
        # Multi-line form
        lines.append(f"    {py_name} = app.{method}(")
        lines.append(f"        {name!r},")
        for kw in kwargs:
            lines.append(f"        {kw},")
        lines.append("    )")
