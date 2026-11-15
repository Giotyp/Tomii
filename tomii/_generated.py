# AUTO-GENERATED — do not edit by hand.
# Regenerate with: make schema
#
# Source: tomii-core/src/json_structs.rs
# Tool:   schemars (Rust) -> datamodel-codegen (Python)
#
# To regenerate after modifying json_structs.rs:
#   make schema
#
# NOTE (instance_map feature): `PredJson` below was hand-patched instead of
# regenerated — the `datamodel-codegen` version available in this environment
# produces a materially different style (drops the `type_`/`Field(alias=...)`
# pattern, switches to `X | None` unions, reorders fields alphabetically,
# wraps `Factor` in a `RootModel`) from what is committed here. Regenerating
# with that version would rewrite this whole file unrelated to the actual
# schema change. Re-run `make schema` with a pinned/matching codegen version
# when one is available, and diff carefully before committing wholesale.
#
from __future__ import annotations

from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Factor: literal integer or variable-name reference (matches Rust's untagged enum)
# ---------------------------------------------------------------------------
Factor = Union[int, str]


# ---------------------------------------------------------------------------
# Condition embedded inside an ArgJson (arg-level init condition)
# ---------------------------------------------------------------------------
class ConditionJson(BaseModel):
    operation: str
    value: str
    value_type: str


# ---------------------------------------------------------------------------
# Predecessor reference used by $res / $barrier / $dep args
# ---------------------------------------------------------------------------
class PredJson(BaseModel):
    name: str
    # Omitted when `instance_map` is used instead.
    indexes: Optional[str] = None
    group_by: Optional[Factor] = None
    # Per-consumer-instance absolute dependency map; mutually exclusive with
    # `indexes` / `group_by`. See `tomii_core::instance_map`.
    instance_map: Optional[List[List[int]]] = None


# ---------------------------------------------------------------------------
# Generic node argument (type tag + optional value / condition / predecessor)
# ---------------------------------------------------------------------------
class ArgJson(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type_: str = Field(alias="type")
    value: Optional[str] = None
    condition: Optional[ConditionJson] = None
    predecessor: Optional[PredJson] = None


# ---------------------------------------------------------------------------
# Initialization argument (always has a value — no predecessor)
# ---------------------------------------------------------------------------
class ArgInit(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type_: str = Field(alias="type")
    value: str  # required (non-optional for init args)


# ---------------------------------------------------------------------------
# Loop configuration attached to a node
# ---------------------------------------------------------------------------
class LoopJson(BaseModel):
    name: str
    factor: Optional[Factor] = None


# ---------------------------------------------------------------------------
# Node-level condition (function + comparison)
# ---------------------------------------------------------------------------
class NodeConditionJson(BaseModel):
    operation: str
    value: str
    value_type: str
    function: str
    args: List[ArgJson]


# ---------------------------------------------------------------------------
# Computation node
# ---------------------------------------------------------------------------
class NodeJson(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    factor: Optional[Factor] = None
    function: str
    loop_: Optional[LoopJson] = Field(None, alias="loop")
    loop_args: Optional[List[ArgJson]] = None
    args: List[ArgJson]
    group_size: Optional[Factor] = None
    condition: Optional[NodeConditionJson] = None
    priority: Optional[str] = None
    use_workers: Optional[str] = None


# ---------------------------------------------------------------------------
# Initialization variable
# ---------------------------------------------------------------------------
class InitJson(BaseModel):
    name: str
    factor: Optional[Factor] = None
    args: List[ArgInit]
    function: Optional[str] = None


# ---------------------------------------------------------------------------
# Network: index-mapping function
# ---------------------------------------------------------------------------
class IndexFunctionJson(BaseModel):
    function: str
    args: List[ArgJson]


# ---------------------------------------------------------------------------
# Network receiver configuration
# ---------------------------------------------------------------------------
class NetworkConfigJson(BaseModel):
    socket_type: str
    num_sockets: Factor
    packet_length: Factor
    frame_packets: Factor
    buffer_depth: int = 128  # default matches Rust's default_buffer_depth()
    address: str
    start_port: Factor
    extract_packet_func: str
    id_function: str
    index_function: IndexFunctionJson


# ---------------------------------------------------------------------------
# Top-level graph file
# ---------------------------------------------------------------------------
class GraphFile(BaseModel):
    initializations: List[InitJson]
    nodes: List[NodeJson]
    post_nodes: Optional[List[NodeJson]] = None
    network_config: Optional[NetworkConfigJson] = None
