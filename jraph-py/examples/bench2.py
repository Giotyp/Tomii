import pyjraph
import numpy as np
from functions import validate_3
from pyjraph import executor


if __name__ == "__main__":
    graph_file = "examples/graphs/bench2.json"
    graph = pyjraph.from_json(graph_file)

    results, comp_load = executor.execute(graph, True)

    print(
        f"Benchmark 2 time for 25 tasks: comp: {comp_load[0]:.2f} seconds, load: {comp_load[1]:.2f} seconds"
    )
