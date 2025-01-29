import pyjraph
import importlib
import sys
import os
import ray

def print_stages(stage_dict):
  for stage in stage_dict:
    print("Stage: ", stage)
    sep = '  '
    for node in stage_dict[stage]:
      print(f"{sep}Node: ", node['name'])
      print(f"{sep*2}Mult Factor: ", node['mult_factor'])
      print(f"{sep*2}Task: ", node['task']['function_name'])
      print(f"{sep*3}Args: ", node['task']['args'])
      print(f"{sep*2}Successors: ", node['successors'])
      print(f"{sep*2}Successors Index: ", node['successors_index'])

def execute(graph):

  # Info for every stage
  functions = None

  num_stages = graph.len()
  stage_dict = { stage: [] for stage in range(num_stages) }

  stage_results = [[] for _ in range(num_stages)]

  for stage in range(num_stages):
    node_names = graph.get_nodes(stage)
    for node in node_names:
      stats = graph.node_info(stage, node)
      task_dict = {
        "function_name": stats['function_name'],
        "args": stats['args'].split(',')
      }
      node_dict = {
        "name": node,
        "mult_factor": int(stats['mult_factor']),
        "task": task_dict,
        "successors": stats['successors_names'].split(','),
        "successors_index": stats['successors_index'].split(','),
      }

      if functions is None:
        function_path = stats['function_path']
        function_file = function_path.split('/')[-1].split('.')[0]
        sys.path.append(function_path)
        functions = importlib.import_module(function_file)

      stage_dict[stage].append(node_dict)

  # Ray Init
  cpu_count = os.cpu_count()
  ray.init(num_cpus=cpu_count//4)

  # The following inits must be done from graph (Init stage)
  size = 1600
  max_concurr = 16

  # Initializations
  fft_buffers = []

  mult_factor = stage_dict[0][0]['mult_factor']

  # Generate buffers in Object Store
  for _ in range(mult_factor):
    buffer_ref = ray.put(functions.generate_set_complex_float_array(size))
    fft_buffers.append(buffer_ref)

  fft_actor = functions.FFTActor.options(max_concurrency=max_concurr).remote(size)

  # Execute graph by scheduling nodes at each stage after predeccors are done

  for stage in range(1):
    nodes = stage_dict[stage]

    for node in nodes:
      node_func = node['task']['function_name'].split('.')
      if len(node_func) == 2:
        # function on an actor
        func = getattr(locals()[node_func[0]], node_func[1])  
      else:
        func = locals()[node_func[0]]

      node_args = node['task']['args']
      if len(node_args) == 0:
        arg_vec = []
      elif len(node_args) == 1:
        arg_vec = locals()[node_args[0]]
      elif len(node_args) > 1:
        arg_vec = [locals()[arg] for arg in node_args]
      else:
        raise ValueError("Number of arguments cannot be less than 0")

      for i in range(node['mult_factor']):

        if len(node_args) == 0:
          compute = func.remote()
        elif len(node_args) == 1:

          if stage == 0:
            # first stage
            compute = func.remote(arg_vec[i])

        stage_results[stage].append(compute)


  # # Compute
  # for i in range(mult_factor):
  #   compute = fft_actor.compute_fft.remote(buffer_refs[i])
  #   stage_res.append(compute)
  #   ids_enum_map[compute] = i

  # # Wait for results
  # while len(stage_res) > 0:
  #   done_id, stage_res = ray.wait(stage_res)

  # Retrieve results from last stage
  results = []
  while len(stage_results[-1]) > 0:
    ready_refs, res_refs = ray.wait(res_refs, num_returns=1, timeout=None)
    results.append(ray.get(ready_refs[0]))

  ray.shutdown()

  return results