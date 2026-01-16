import re
from utils import parse_unit_value, convert_to_ns, convert_from_ns, format_unit_value

class TimingFile:
    """Represents a parsed timing file."""
    
    def __init__(self):
        self.filename = ""
        self.total_slots = 0
        self.timing_method = ""
        self.worker_slots_range = ""
        self.system_slots_range = ""
        
        # Aggregated statistics
        self.total_streams = 0
        self.streams_per_slot = {}
        self.total_runtime = (0.0, 'ms')
        self.avg_time_per_stream = (0.0, 'ms')
        self.min_max_per_stream = ((0.0, 'ms'), (0.0, 'ms'))
        self.total_compute_time = (0.0, 'µs')
        self.avg_compute_time_per_stream = (0.0, 'µs')
        
        # Per-task analysis
        self.tasks = []  # List of task dictionaries
        
        # System thread tasks
        self.system_threads = []  # List of system thread dictionaries
    
    def parse(self, filepath):
        """Parse a timing file and extract all metrics."""
        self.filename = filepath
        
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Parse header
            if line.startswith('Time Statistics for'):
                i += 1
                continue
            
            if line.startswith('Total Slots:'):
                self.total_slots = int(line.split(':')[1].strip())
                i += 1
                continue
            
            if line.startswith('Timing Method:'):
                self.timing_method = line.split(':')[1].strip()
                i += 1
                continue
            
            if line.startswith('Worker Slots:'):
                match = re.search(r'Worker Slots: ([\d.]+), System Thread Slots: ([\d.]+)', line)
                if match:
                    self.worker_slots_range = match.group(1)
                    self.system_slots_range = match.group(2)
                i += 1
                continue
            
            # Parse aggregated statistics
            if line.startswith('Total Streams Processed:'):
                self.total_streams = int(line.split(':')[1].strip())
                i += 1
                continue
            
            if line.startswith('Streams per Slot:'):
                # Parse slot info (e.g., "Slot 0: 1")
                slot_info = line.split(':', 1)[1].strip()
                for slot_pair in slot_info.split(','):
                    if 'Slot' in slot_pair:
                        slot_match = re.search(r'Slot (\d+): (\d+)', slot_pair)
                        if slot_match:
                            self.streams_per_slot[int(slot_match.group(1))] = int(slot_match.group(2))
                i += 1
                continue
            
            if line.startswith('Total Runtime:'):
                value_str = line.split(':')[1].strip()
                value, unit = parse_unit_value(value_str)
                self.total_runtime = (value, unit)
                i += 1
                continue
            
            if line.startswith('Avg Time Per Stream:'):
                value_str = line.split(':')[1].strip()
                value, unit = parse_unit_value(value_str)
                self.avg_time_per_stream = (value, unit)
                i += 1
                continue
            
            if line.startswith('Min/Max Per Stream:'):
                rest = line.split(':')[1].strip()
                parts = rest.split('/')
                min_val, min_unit = parse_unit_value(parts[0].strip())
                max_val, max_unit = parse_unit_value(parts[1].strip())
                self.min_max_per_stream = ((min_val, min_unit), (max_val, max_unit))
                i += 1
                continue
            
            if line.startswith('Total Compute Time:'):
                value_str = line.split(':')[1].strip()
                value, unit = parse_unit_value(value_str)
                self.total_compute_time = (value, unit)
                i += 1
                continue
            
            if line.startswith('Avg Compute Time Per Stream:'):
                value_str = line.split(':')[1].strip()
                value, unit = parse_unit_value(value_str)
                self.avg_compute_time_per_stream = (value, unit)
                i += 1
                continue
            
            # Parse per-task analysis
            if line.startswith("Task '") and " - Workers:" in line:
                task = self.parse_task(lines, i)
                if task:
                    self.tasks.append(task)
                i += 1
                continue
            
            # Parse system thread tasks
            if line.startswith('Resolution Thread') and '(Slot' in line:
                sys_thread = self.parse_system_thread(lines, i)
                if sys_thread:
                    self.system_threads.append(sys_thread)
                i += 1
                continue
            
            i += 1
    
    def parse_task(self, lines, start_idx):
        """Parse a task section."""
        line = lines[start_idx].strip()
        
        # Parse task name and info
        match = re.search(r"Task '([^']+)' - Workers: (\d+), Total Executions: (\d+)", line)
        if not match:
            return None
        
        task = {
            'name': match.group(1),
            'workers': int(match.group(2)),
            'executions': int(match.group(3)),
            'timing': {},
            'worker_summary': {}
        }
        
        # Parse timing line (next line)
        if start_idx + 1 < len(lines):
            timing_line = lines[start_idx + 1].strip()
            if timing_line.startswith('Timing -'):
                # Extract timing metrics - flexible unit matching
                avg_stream_match = re.search(r'Avg/Stream: ([\d.]+)(µs|ns|ms|s)', timing_line)
                avg_task_match = re.search(r'Avg/Task: ([\d.]+)(µs|ns|ms|s)', timing_line)
                min_match = re.search(r'Min: ([\d.]+)(µs|ns|ms|s)', timing_line)
                max_match = re.search(r'Max: ([\d.]+)(µs|ns|ms|s)', timing_line)
                total_match = re.search(r'Total: ([\d.]+)(µs|ns|ms|s)', timing_line)
                
                if avg_stream_match:
                    task['timing']['avg_stream'] = (float(avg_stream_match.group(1)), avg_stream_match.group(2))
                if avg_task_match:
                    task['timing']['avg_task'] = (float(avg_task_match.group(1)), avg_task_match.group(2))
                if min_match:
                    task['timing']['min'] = (float(min_match.group(1)), min_match.group(2))
                if max_match:
                    task['timing']['max'] = (float(max_match.group(1)), max_match.group(2))
                if total_match:
                    task['timing']['total'] = (float(total_match.group(1)), total_match.group(2))
        
        # Parse worker summary line
        if start_idx + 2 < len(lines):
            worker_line = lines[start_idx + 2].strip()
            if worker_line.startswith('Worker Summary:'):
                # Extract worker info - flexible unit matching
                # Support both "W-0: 92 (46.0%) - 67.1970µs" and "0: 92 (46.0%) - 67.1970µs"
                workers = re.findall(r'(?:W-)?(\d+): (\d+) \(([\d.]+)%\) - ([\d.]+)(µs|ns|ms|s)', worker_line)
                for w_id, count, percent, value, unit in workers:
                    task['worker_summary'][int(w_id)] = {
                        'count': int(count),
                        'percent': float(percent),
                        'time': (float(value), unit)
                    }
        
        return task
    
    def parse_system_thread(self, lines, start_idx):
        """Parse a system thread section."""
        line = lines[start_idx].strip()
        
        # Parse system thread header
        match = re.search(r'Resolution Thread (\d+) \(Slot (\d+)\):', line)
        if not match:
            return None
        
        sys_thread = {
            'thread_id': int(match.group(1)),
            'slot': int(match.group(2)),
            'tasks': []
        }
        
        # Parse system tasks (next lines)
        i = start_idx + 1
        while i < len(lines):
            task_line = lines[i].strip()
            if not task_line or task_line.startswith('*') or task_line.startswith('Resolution Thread'):
                break
            
            if task_line.startswith("Task '"):
                # Parse system task line - flexible unit matching
                task_match = re.search(
                    r"Task '([^']+)' - Executions: (\d+), Avg: ([\d.]+)(µs|ns|ms|s), "
                    r"Min: ([\d.]+)(µs|ns|ms|s), Max: ([\d.]+)(µs|ns|ms|s), Total: ([\d.]+)(µs|ns|ms|s)",
                    task_line
                )
                if task_match:
                    sys_task = {
                        'name': task_match.group(1),
                        'executions': int(task_match.group(2)),
                        'avg': (float(task_match.group(3)), task_match.group(4)),
                        'min': (float(task_match.group(5)), task_match.group(6)),
                        'max': (float(task_match.group(7)), task_match.group(8)),
                        'total': (float(task_match.group(9)), task_match.group(10))
                    }
                    sys_thread['tasks'].append(sys_task)
            i += 1
        
        return sys_thread