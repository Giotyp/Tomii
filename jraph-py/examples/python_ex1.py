import pyjraph

if __name__ == "__main__":
    graph_file = "examples/graphs/graph.json"
    graph = pyjraph.from_json(graph_file)

    print("Graph class: ", graph.__class__)
    print("Graph functions: ", dir(graph))
    print()

    graph.print_graph()
