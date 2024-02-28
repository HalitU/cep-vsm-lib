class ScoreWeights:
    def __init__(self):
        self.current_data_read_ns: float = 0.0
        self.current_data_write_ns: float = 0.0
        self.current_data_delete_ns: float = 0.0
        self.current_task_execution_ns: float = 0.0
        self.current_in_data_byte: float = 0.0
        self.current_out_data_byte: float = 0.0
        self.current_execution_elapsed_ms: float = 0.0
        self.max_ram_usage: float = 0.0
        self.max_cpu_usage: float = 0.0
