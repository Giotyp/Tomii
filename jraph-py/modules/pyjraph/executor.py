import pyjraph
import importlib
import sys
import os
import ray


def print_stages(stage_dict):
    for stage in stage_dict:
        print("Stage: ", stage)
        sep = "  "
        for node in stage_dict[stage]:
            print(f"{sep}Node: ", node["name"])
            print(f"{sep*2}Mult Factor: ", node["mult_factor"])
            print(f"{sep*2}Task: ", node["task"]["function_name"])
            print(f"{sep*3}Args: ", node["task"]["args"])
            print(f"{sep*2}Successors: ", node["successors"])
            print(f"{sep*2}Successors Index: ", node["successors_index"])


def execute(graph):

    # Info for every stage
    functions = None

    num_stages = graph.len()
    stage_dict = {stage: [] for stage in range(num_stages)}

    for stage in range(num_stages):
        node_names = graph.get_nodes(stage)
        for node in node_names:
            stats = graph.node_info(stage, node)
            task_dict = {
                "function_name": stats["function_name"],
                "args": stats["args"].split(","),
            }
            node_dict = {
                "name": node,
                "mult_factor": int(stats["mult_factor"]),
                "task": task_dict,
                "successors": stats["successors_names"].split(","),
                "successors_index": stats["successors_index"].split(","),
            }

            if functions is None:
                function_path = stats["function_path"]
                function_file = function_path.split("/")[-1].split(".")[0]
                sys.path.append(function_path)
                functions = importlib.import_module(function_file)
                # import all functions from funtions
                globals().update(
                    {
                        name: getattr(functions, name)
                        for name in dir(functions)
                        if callable(getattr(functions, name))
                    }
                )

            stage_dict[stage].append(node_dict)

    # Ray Init
    cpu_count = os.cpu_count()
    ray.init(num_cpus=cpu_count // 4)

    # The following inits must be done from graph (Init stage)
    size = 1600
    max_concurr = 16

    # Initializations
    fft_buffers = []

    mult_factor = stage_dict[0][0]["mult_factor"]

    # Generate buffers in Object Store
    for _ in range(mult_factor):
        buffer_ref = ray.put(functions.generate_set_complex_float_array(size))
        fft_buffers.append(buffer_ref)

    fft_actor = functions.FFTActor.options(max_concurrency=max_concurr).remote(size)

    # Execute graph by scheduling nodes at each stage after predeccors are done
    stage_results = [[] for _ in range(num_stages)]
    scheduled_ids = [{} for _ in range(num_stages)]
    completed_ids = [[] for _ in range(num_stages)]

    successors_stage = []

    barrier_sched = {
        # barrier only for stage 1
        1: set(range(mult_factor))
    }

    for stage in range(num_stages):
        nodes = stage_dict[stage]

        for node in nodes:
            node_func = node["task"]["function_name"].split(".")
            if len(node_func) == 2:
                # function on an actor
                func = getattr(locals()[node_func[0]], node_func[1])
            else:
                func = globals()[node_func[0]]

            if stage != num_stages - 1:
                successors_index = node["successors_index"]
                successors_stage.append([int(succ) for succ in successors_index])

            node_args = node["task"]["args"]

            arg_types = graph.get_arg_types(stage, node["name"])

            if stage == 0:

                arg_vec = []
                for arg in node_args:
                    arg_vec.append(locals()[arg])

                for i in range(node["mult_factor"]):

                    if len(node_args) == 0:
                        compute = func.remote()
                    elif len(node_args) == 1:
                        # first stage
                        compute = func.remote(arg_vec[0][i])

                    stage_results[stage].append(compute)
                    scheduled_ids[stage][compute] = i
            elif "$ref" in arg_types:

                arg_vec = []
                for arg in node_args:
                    arg_vec.append(locals()[arg])
                    
                # barrier here for $ref arg type
                while len(barrier_sched[stage]) > 0:
                    ready_refs, stage_results[stage - 1] = ray.wait(
                        stage_results[stage - 1], num_returns=1, timeout=None
                    )
                    index = scheduled_ids[stage - 1][ready_refs[0]]
                    completed_ids[stage - 1].append(index)

                    successors = successors_stage[stage - 1]

                    scheduled = []
                    for sched in barrier_sched[stage]:
                        if sched in completed_ids[stage - 1]:
                            if len(node_args) == 0:
                                compute = func.remote()
                            elif len(node_args) == 1:
                                call_args = [arg_vec[0][sched - succ_idx] for succ_idx in successors]
                                compute = func.remote(*call_args)

                            stage_results[stage].append(compute)
                            scheduled_ids[stage][compute] = i
                            scheduled.append(sched)
                    barrier_sched[stage].difference_update(scheduled)
            elif "$res" in arg_types:
                # no barrier for $res
                successors = successors_stage[stage - 1]
                for i in range(node["mult_factor"]):
                    if len(node_args) == 0:
                        compute = func.remote()
                    elif len(node_args) == 1:
                        # first stage
                        call_args = [stage_results[stage - 1][i - succ_idx] for succ_idx in successors]
                        compute = func.remote(*call_args)

                        stage_results[stage].append(compute)

    # Retrieve results from last stage
    results = []
    while len(stage_results[-1]) > 0:
        ready_refs, stage_results[-1] = ray.wait(
            stage_results[-1], num_returns=1, timeout=None
        )
        results.append(ray.get(ready_refs[0]))

    ray.shutdown()

    return results
