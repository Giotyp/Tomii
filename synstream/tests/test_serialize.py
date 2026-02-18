"""Tests for JSON serialization — validates against examples/matrix-compute/graph.json."""

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import synstream as ss
from synstream._serialize import serialize_arg, serialize_node, serialize_var
from synstream._types import infer_type
from synstream._var import Var


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

GRAPH_JSON = REPO_ROOT / "examples" / "matrix-compute" / "graph.json"


def load_reference() -> dict:
    return json.loads(GRAPH_JSON.read_text())


def build_matrix_compute_graph() -> ss.Graph:
    """Build the matrix-compute example graph using the Python API."""
    app = ss.Graph()

    buf_size = app.var("buf_size", 100)
    num_nodes = app.var("num_nodes", 200)
    fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])
    result_file = app.var(
        "result_file",
        func="get_out_file",
        args=[ss.String("SCRIPT_DIR"), ss.String("result.txt")],
    )

    gen_vec = app.node(
        "gen_vec", func="generate_vector", factor=num_nodes, args=[buf_size]
    )
    compute_fft = app.node(
        "compute_fft",
        func="compute_fft",
        factor=num_nodes,
        args=[fft_planner, gen_vec.out(0)],
    )
    vec_mat = app.node(
        "vec_mat",
        func="vec_to_mat",
        factor=num_nodes,
        args=[gen_vec.out(0), compute_fft.wait(0)],
    )
    mat_mul = app.node(
        "mat_mul",
        func="mat_mul",
        factor=num_nodes,
        args=[vec_mat.out(0), vec_mat.out(0)],
    )
    _write_res = app.node(
        "write_res", func="write_to_file", args=[result_file, mat_mul.out(0, num_nodes)]
    )

    return app


# --------------------------------------------------------------------------- #
# Serialization correctness against graph.json
# --------------------------------------------------------------------------- #


class TestMatrixComputeGraph:
    def setup_method(self):
        self.app = build_matrix_compute_graph()
        self.got = json.loads(self.app.to_json())
        self.ref = load_reference()

    def test_top_level_keys(self):
        assert set(self.got.keys()) == set(self.ref.keys())

    def test_initializations_count(self):
        assert len(self.got["initializations"]) == len(self.ref["initializations"])

    def test_nodes_count(self):
        assert len(self.got["nodes"]) == len(self.ref["nodes"])

    def test_buf_size_init(self):
        got_init = next(i for i in self.got["initializations"] if i["name"] == "buf_size")
        ref_init = next(i for i in self.ref["initializations"] if i["name"] == "buf_size")
        assert got_init == ref_init

    def test_num_nodes_init(self):
        got_init = next(i for i in self.got["initializations"] if i["name"] == "num_nodes")
        ref_init = next(i for i in self.ref["initializations"] if i["name"] == "num_nodes")
        assert got_init == ref_init

    def test_fft_planner_init(self):
        got_init = next(i for i in self.got["initializations"] if i["name"] == "fft_planner")
        ref_init = next(i for i in self.ref["initializations"] if i["name"] == "fft_planner")
        assert got_init == ref_init

    def test_result_file_init(self):
        got_init = next(i for i in self.got["initializations"] if i["name"] == "result_file")
        ref_init = next(i for i in self.ref["initializations"] if i["name"] == "result_file")
        assert got_init == ref_init

    def test_gen_vec_node(self):
        got_node = next(n for n in self.got["nodes"] if n["name"] == "gen_vec")
        ref_node = next(n for n in self.ref["nodes"] if n["name"] == "gen_vec")
        assert got_node == ref_node

    def test_compute_fft_node(self):
        got_node = next(n for n in self.got["nodes"] if n["name"] == "compute_fft")
        ref_node = next(n for n in self.ref["nodes"] if n["name"] == "compute_fft")
        assert got_node == ref_node

    def test_vec_mat_node(self):
        got_node = next(n for n in self.got["nodes"] if n["name"] == "vec_mat")
        ref_node = next(n for n in self.ref["nodes"] if n["name"] == "vec_mat")
        assert got_node == ref_node

    def test_mat_mul_node(self):
        got_node = next(n for n in self.got["nodes"] if n["name"] == "mat_mul")
        ref_node = next(n for n in self.ref["nodes"] if n["name"] == "mat_mul")
        assert got_node == ref_node

    def test_write_res_node(self):
        got_node = next(n for n in self.got["nodes"] if n["name"] == "write_res")
        ref_node = next(n for n in self.ref["nodes"] if n["name"] == "write_res")
        assert got_node == ref_node

    def test_full_graph_equality(self):
        """The full serialized graph must match the reference JSON exactly."""
        assert self.got == self.ref


# --------------------------------------------------------------------------- #
# Type inference tests
# --------------------------------------------------------------------------- #


class TestTypeInference:
    def test_int_infers_usize(self):
        tv = infer_type(42)
        assert tv.type_name == "usize"
        assert tv.value_str == "42"

    def test_float_infers_f64(self):
        tv = infer_type(3.14)
        assert tv.type_name == "f64"
        assert tv.value_str == "3.14"

    def test_bool_infers_bool_true(self):
        tv = infer_type(True)
        assert tv.type_name == "bool"
        assert tv.value_str == "true"

    def test_bool_infers_bool_false(self):
        tv = infer_type(False)
        assert tv.type_name == "bool"
        assert tv.value_str == "false"

    def test_string_raises(self):
        with pytest.raises(TypeError):
            infer_type("hello")

    def test_string_wrapper(self):
        tv = ss.String("hello")
        assert tv.type_name == "String"
        assert tv.value_str == "hello"

    def test_i32_wrapper(self):
        tv = ss.i32(-5)
        assert tv.type_name == "i32"
        assert tv.value_str == "-5"

    def test_vec_wrapper(self):
        tv = ss.Vec("usize", [1, 2, 3])
        assert tv.type_name == "Vec<usize>"
        assert tv.value_str == "1,2,3"


# --------------------------------------------------------------------------- #
# Factor serialization
# --------------------------------------------------------------------------- #


class TestFactor:
    def test_int_factor(self):
        app = ss.Graph()
        n = app.node("x", func="f", factor=10, args=[])
        d = serialize_node(n)
        assert d["factor"] == 10

    def test_var_factor(self):
        app = ss.Graph()
        v = app.var("count", 5)
        n = app.node("x", func="f", factor=v, args=[])
        d = serialize_node(n)
        assert d["factor"] == "count"

    def test_no_factor_omitted(self):
        app = ss.Graph()
        n = app.node("x", func="f", args=[])
        d = serialize_node(n)
        assert "factor" not in d


# --------------------------------------------------------------------------- #
# Index specifications
# --------------------------------------------------------------------------- #


class TestIndexes:
    def test_single_index(self):
        app = ss.Graph()
        n = app.node("src", func="f", args=[])
        out = n.out(0)
        d = serialize_arg(out)
        assert d["predecessor"]["indexes"] == "0"

    def test_range_index(self):
        app = ss.Graph()
        n = app.node("src", func="f", args=[])
        out = n.out(0, 99)
        d = serialize_arg(out)
        assert d["predecessor"]["indexes"] == "0-99"

    def test_var_range_index(self):
        app = ss.Graph()
        count = app.var("count", 10)
        n = app.node("src", func="f", args=[])
        out = n.out(0, count)
        d = serialize_arg(out)
        assert d["predecessor"]["indexes"] == "0-count"

    def test_list_index(self):
        app = ss.Graph()
        n = app.node("src", func="f", args=[])
        out = n.out([0, 5, 10])
        d = serialize_arg(out)
        assert d["predecessor"]["indexes"] == "0,5,10"

    def test_barrier(self):
        app = ss.Graph()
        n = app.node("src", func="f", args=[])
        barrier = n.wait(0)
        d = serialize_arg(barrier)
        assert d["type"] == "$barrier"
        assert d["predecessor"]["indexes"] == "0"

    def test_barrier_group_by(self):
        app = ss.Graph()
        n = app.node("src", func="f", args=[])
        barrier = n.wait(0, group_by=64)
        d = serialize_arg(barrier)
        assert d["predecessor"]["group_by"] == 64


# --------------------------------------------------------------------------- #
# Graph construction validation
# --------------------------------------------------------------------------- #


class TestGraphValidation:
    def test_duplicate_var_name(self):
        app = ss.Graph()
        app.var("x", 1)
        with pytest.raises(ValueError, match="Duplicate"):
            app.var("x", 2)

    def test_duplicate_node_name(self):
        app = ss.Graph()
        app.node("n", func="f", args=[])
        with pytest.raises(ValueError, match="Duplicate"):
            app.node("n", func="g", args=[])

    def test_var_without_value_or_func(self):
        with pytest.raises(ValueError):
            Var("x")

    def test_var_with_both_value_and_func(self):
        with pytest.raises(ValueError):
            Var("x", value=1, func="f")
