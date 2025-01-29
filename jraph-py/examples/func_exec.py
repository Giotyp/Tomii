import pyjraph

if __name__ == "__main__":
    graph_file = "examples/graphs/graph.json"
    graph = pyjraph.from_json(graph_file)

    size = graph.len()

    nodes = graph.get_nodes(1)
    print("Nodes: ", nodes)
    print("Graph size: ", size)

    func = graph.get_func(1, nodes[0])
    print("Function: ", func)

    result = graph.execute(1, nodes[0])