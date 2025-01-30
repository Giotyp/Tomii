import pyjraph
import numpy as np
from pyjraph import executor


fft_buffers = []

if __name__ == "__main__":
    graph_file = "examples/graphs/bench1.json"
    graph = pyjraph.from_json(graph_file)

    # fft_actor = FFTActor.options(max_concurrency=16).remote(size)
    results = executor.execute(graph)
    print(f"results type: {type(results)}")
    print(f"results[0] type: {type(results[0])}")
    print(f"results length: {len(results)}")
    print(f"Len Results: {len(results)}-{results[0].shape}")