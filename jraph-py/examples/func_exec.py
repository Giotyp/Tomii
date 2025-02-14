import pyjraph

if __name__ == "__main__":
    graph_file = "examples/graphs/graph.json"
    graph = pyjraph.from_json(graph_file)

    nodes = graph.get_nodes(1)

    func = graph.get_func(1, nodes[0])
    print("Function: ", func)

    graph.print_graph()
