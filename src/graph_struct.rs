struct Task {
    args: i32,
    output: bool,
}

impl Task {
    fn new(args: i32, output: bool) -> Task {
        Task { args, output }
    }
}

struct Node {
    name: String,
    task: Task,
    successors: Vec<&Node>,
    dependents: Vec<&Node>,
}

impl Node {
    fn new(name: String, task: Task, successors: Vec<&Node>, dependents: Vec<&Node>) -> Node {
        Node {
            name,
            task,
            successors,
            dependents,
        }
    }
}

struct Graph {
    nodes: Vec<Node>,
}

impl Graph {
    fn new() -> Graph {
        Graph { nodes: Vec::new() }
    }

    fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }
}
