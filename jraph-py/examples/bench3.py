import pyjraph
import numpy as np
from functions import validate_3
from pyjraph import executor


if __name__ == "__main__":
    graph_file = "examples/graphs/bench3.json"
    graph = pyjraph.from_json(graph_file)

    results = executor.execute(graph)

    reference = validate_3(1600, 5)
    for i in range(5):
        assert np.allclose(results[i], reference[i], atol=1e-6)
