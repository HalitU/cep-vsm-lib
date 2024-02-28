import math
import sys

class TopicStatisticDetail:
    def __init__(self, data_process_time:float=0.0, data_size:float=0.0):
        self.data_process_time: float = data_process_time
        self.data_size: float = data_size
        self.local: bool = False

class IndividualTopicStatistics:
    def __init__(self, topic_name) -> None:
        self.topic_name = topic_name
        # type 1
        self.topics_override_count = 0
        # type 2
        self.topics_new_count = 0
        # type 3
        self.topics_dead_on_arrival_count = 0
        # type 4
        self.topics_old_arrival_count = 0
        # type 5
        self.topics_other_arrival_count = 0
        
        self.hit_count = 0
        self.miss_count = 0

    def update_stat(self, data_lifetime_status_type, hit:bool):
        if data_lifetime_status_type == 1:
            self.topics_override_count += 1
        elif data_lifetime_status_type == 2:
            self.topics_new_count += 1
        elif data_lifetime_status_type == 3:
            self.topics_dead_on_arrival_count += 1
        elif data_lifetime_status_type == 4:
            self.topics_old_arrival_count += 1
        elif data_lifetime_status_type == 5:
            self.topics_other_arrival_count += 1
        else:
            raise Exception("INVALID LIFETIME STATUS RECEIVED!!!!!!!!!!!!!!!!!!")

        if hit == True:
            self.hit_count += 1
        else:
            self.miss_count += 1

class SingleStatisticsRecord:
    def __init__(self):
        self.action_name = ""
        self.add_action_end = 0.0
        self.data_read_time = 0.0
        self.data_write_time = 0.0
        self.data_delete_time = 0.0
        self.total_elapsed = 0.0
        self.task_execution_time = 0.0
        self.in_data_byte = 0.0
        self.out_data_byte = 0.0
        self.execution_elapsed_ms = 0.0
        self.process_cpu_usage = 0.0
        self.process_memory_usage = 0.0
        self.current_bandwdith_usage = 0.0
        self.in_topic_records = {}
        self.in_local_data_byte = 0.0
        self.in_remote_data_byte = 0.0
        self.out_local_data_byte = 0.0
        self.out_remote_data_byte = 0.0
        self.data_lifetime_status_type = None
        self.raw_elapsed_time_from_init_ms = 0.0
        self.raw_elapsed_max_time_from_init_ms = 0.0
        self.raw_elapsed_min_time_from_init_ms = 0.0

# a nice implementation of counters
# https://stackoverflow.com/questions/56710991/calculate-standard-deviation-%CF%83-from-the-previous-one-and-a-new-element-cumula
class StatisticValue:
    def __init__(self, name) -> None:
        self.name = name
        self.count:float = 0.0
        self.total:float = 0.0
        self.min:float = None
        self.max:float = 0.0
        self.avg:float = 0.0
        self.std:float = 0.0
        self.var:float = 0.0
        
    def add(self, val:float):        
        self.count += 1
        self.total += val
        if self.min is None:
            self.min = val
            self.max = val
            self.avg = self.total * 1.0 / self.count
            self.var = 0.0
            self.std = 0.0
        else:
            self.min = min([self.min, val])
            self.max = max([self.max, val])
            old_avg = self.avg
            self.avg = self.total * 1.0 / self.count
            self.var = ((self.count - 1) * self.var + (val - self.avg) * (val - old_avg)) / self.count
            self.std = math.sqrt(self.var)      
                
    # used to add continously increasing number differences
    def increment(self, val:float):        
        self.count += 1
        if self.min is None:
            self.total += val
            self.min = val
            self.max = val
            self.avg = val
            self.var = 0.0
            self.std = 0.0
        else:
            old_max = self.max
            self.max = val
            val = val - old_max
            if val < 0.0:
                print("FAAAAAAAAAAAATTTTAAAAAAAAAAAAAAAAL ERRROOOOOOOOOOOOOOOOOOOOOOR during increment statistics!!!")
                sys.exit(0)
                raise Exception("FAAAAAAAAAAAATTTTAAAAAAAAAAAAAAAAL ERRROOOOOOOOOOOOOOOOOOOOOOR during increment statistics!!!")
            self.total += val
            old_avg = self.avg
            self.avg = self.total * 1.0 / self.count
            self.var = ((self.count - 1) * self.var + (val - self.avg) * (val - old_avg)) / self.count
            self.std = math.sqrt(self.var)
        
        return val
        
    def reset(self):        
        self.count = 0.0
        self.total = 0.0
        self.min = None
        self.max = 0.0
        self.avg = 0.0
        self.std = 0.0
        self.var = 0.0
                      
    def soft_reset(self):        
        self.count = 0.0
        self.total = 0.0
        self.min = self.max
        self.max = self.max
        self.avg = 0.0
        self.std = 0.0
        self.var = 0.0
                      
    def header(self) -> str:
        return "count,total,min,max,avg,var,std"
        
    def __str__(self) -> str:
        return ",".join([str(self.count), str(self.total), str(self.min), str(self.max), 
                         str(self.avg), str(self.var), str(self.std)])

class StatisticRecord:
    def __init__(self, action_name:str = "", activation_time_ns:float = 0.0):
        self.action_name: str = action_name
        self.activation_time_ns: float = activation_time_ns
        self.miss_count = 0.0
        self.hit_count = 0.0

        self.in_topic_stats = {}
        self.topic_cpu_stats = {}
        self.individual_topic_stats = {}
        self.in_topic_data_size_stats = {}
        self.in_topic_data_read_time_stats = {}
        self.in_topic_local_read_count = {}
        self.in_topic_remote_read_count = {}
        
        self.crr_total_elapsed_s_stats = StatisticValue("total_elapsed_s")
        self.crr_data_read_ns_stats = StatisticValue("data_read_ns")
        self.crr_data_write_ns_stats = StatisticValue("data_write_ns")
        self.crr_data_delete_ns_stats = StatisticValue("data_delete_ns")
        self.crr_task_execution_ns_stats = StatisticValue("task_execution_ns")
        self.crr_in_data_byte_stats = StatisticValue("in_data_byte")
        self.crr_out_data_byte_stats = StatisticValue("out_data_byte")
        self.crr_execution_elapsed_ms_stats = StatisticValue("execution_elapsed_ms")
        self.crr_current_bandwdith_usage_stats = StatisticValue("current_bandwdith_usage")
        self.crr_in_local_data_byte_stats = StatisticValue("in_local_data_byte")
        self.crr_in_remote_data_byte_stats = StatisticValue("in_remote_data_byte")
        self.crr_out_local_data_byte_stats = StatisticValue("out_local_data_byte")
        self.crr_out_remote_data_byte_stats = StatisticValue("out_remote_data_byte")
        self.crr_cpu_usage_stats = StatisticValue("cpu_usage")        
        self.crr_memory_usage_stats = StatisticValue("memory_usage")       
        self.crr_raw_elapsed_time_from_init_ms = StatisticValue("raw_elapsed_time_from_init_ms")
        self.crr_raw_elapsed_max_time_from_init_ms = StatisticValue("raw_elapsed_max_time_from_init_ms")
        self.crr_raw_elapsed_min_time_from_init_ms = StatisticValue("raw_elapsed_min_time_from_init_ms")
        
        self.total_elapsed_s_stats = StatisticValue("total_elapsed_s")
        self.data_read_ns_stats = StatisticValue("data_read_ns")
        self.data_write_ns_stats = StatisticValue("data_write_ns")
        self.data_delete_ns_stats = StatisticValue("data_delete_ns")
        self.task_execution_ns_stats = StatisticValue("task_execution_ns")
        self.in_data_byte_stats = StatisticValue("in_data_byte")
        self.out_data_byte_stats = StatisticValue("out_data_byte")
        self.execution_elapsed_ms_stats = StatisticValue("execution_elapsed_ms")
        self.current_bandwdith_usage_stats = StatisticValue("current_bandwdith_usage")
        self.in_local_data_byte_stats = StatisticValue("in_local_data_byte")
        self.in_remote_data_byte_stats = StatisticValue("in_remote_data_byte")
        self.out_local_data_byte_stats = StatisticValue("out_local_data_byte")
        self.out_remote_data_byte_stats = StatisticValue("out_remote_data_byte")
        self.cpu_usage_stats = StatisticValue("cpu_usage")
        self.memory_usage_stats = StatisticValue("memory_usage")     
        self.total_raw_elapsed_time_from_init_ms = StatisticValue("total_raw_elapsed_time_from_init_ms")
        self.total_raw_elapsed_max_time_from_init_ms = StatisticValue("raw_elapsed_max_time_from_init_ms")
        self.total_raw_elapsed_min_time_from_init_ms = StatisticValue("raw_elapsed_min_time_from_init_ms")
        
    def reset_cpu_stats(self):
        self.crr_cpu_usage_stats.reset()
        for topic_n in self.topic_cpu_stats:
            self.topic_cpu_stats[topic_n] = 0.0
        
    def reset_stats(self):
        self.crr_total_elapsed_s_stats.reset()
        self.crr_data_read_ns_stats.reset()
        self.crr_data_write_ns_stats.reset()
        self.crr_data_delete_ns_stats.reset()
        self.crr_task_execution_ns_stats.reset()
        self.crr_in_data_byte_stats.reset()
        self.crr_out_data_byte_stats.reset()
        self.crr_execution_elapsed_ms_stats.reset()
        self.crr_current_bandwdith_usage_stats.reset()
        self.crr_in_local_data_byte_stats.reset()
        self.crr_in_remote_data_byte_stats.reset()
        self.crr_out_local_data_byte_stats.reset()
        self.crr_out_remote_data_byte_stats.reset()
        self.crr_cpu_usage_stats.soft_reset()
        self.crr_memory_usage_stats.reset()
        self.crr_raw_elapsed_time_from_init_ms.reset()
        self.crr_raw_elapsed_max_time_from_init_ms.reset()
        self.crr_raw_elapsed_min_time_from_init_ms.reset()

        val:StatisticValue
        for _, val in self.in_topic_data_size_stats.items():
            val.reset()
        for _, val in self.in_topic_data_read_time_stats.items():
            val.reset()

        for key in self.in_topic_local_read_count:
            self.in_topic_local_read_count[key] = 0
        for key in self.in_topic_remote_read_count:
            self.in_topic_remote_read_count[key] = 0

    def update_topic_stats(self, data_lifetime_status_type, triggering_topic_name:str, hit:bool):
        # Individual topic records
        if data_lifetime_status_type != None:
            if triggering_topic_name in self.individual_topic_stats:
                self.individual_topic_stats[triggering_topic_name].update_stat(data_lifetime_status_type, hit)
            else:
                self.individual_topic_stats[triggering_topic_name] = IndividualTopicStatistics(triggering_topic_name)
                self.individual_topic_stats[triggering_topic_name].update_stat(data_lifetime_status_type, hit)

    def update_stats(self, ssr: SingleStatisticsRecord, triggering_topic_name:str):
        vals:TopicStatisticDetail
        for topic_name, vals in ssr.in_topic_records.items():            
            if topic_name in self.in_topic_stats:
                topic_stat_count = self.in_topic_stats[topic_name]["count"]
                topic_stats:TopicStatisticDetail = self.in_topic_stats[topic_name]["stats"]
                
                topic_stats.data_process_time = (topic_stats.data_process_time * topic_stat_count + vals.data_process_time) / (topic_stat_count + 1.0)
                topic_stats.data_size = (topic_stats.data_size * topic_stat_count + vals.data_size) / (topic_stat_count + 1.0)
                
                self.in_topic_stats[topic_name]["count"] = topic_stat_count + 1.0
                self.in_topic_stats[topic_name]["stats"] = topic_stats
            else:
                self.in_topic_stats[topic_name] = {}
                self.in_topic_stats[topic_name]["count"] = 1.0
                self.in_topic_stats[topic_name]["stats"] = vals

            if topic_name in self.in_topic_data_size_stats:
                topic_data_size_stats: StatisticValue = self.in_topic_data_size_stats[topic_name]
                topic_data_read_time_stats: StatisticValue = self.in_topic_data_read_time_stats[topic_name]
                topic_data_size_stats.add(vals.data_size)
                topic_data_read_time_stats.add(vals.data_process_time)
                if vals.local:
                    self.in_topic_local_read_count[topic_name] += 1
                else:
                    self.in_topic_remote_read_count[topic_name] += 1                
            else:
                self.in_topic_data_size_stats[topic_name] = StatisticValue(topic_name)
                self.in_topic_data_read_time_stats[topic_name] = StatisticValue(topic_name)
                self.in_topic_local_read_count[topic_name] = 0
                self.in_topic_remote_read_count[topic_name] = 0
                if vals.local:
                    self.in_topic_local_read_count[topic_name] += 1
                else:
                    self.in_topic_remote_read_count[topic_name] += 1
                    
        # it is possible that any previous execution of a thread can trigger this stats somehow
        # print("--------------------------------")
        # print("Current topic cpu usages: ", self.topic_cpu_stats)
        old_thread_cpu_usage = 0.0 if triggering_topic_name not in self.topic_cpu_stats else self.topic_cpu_stats[triggering_topic_name]
        # print("Old cpu usage: ", old_thread_cpu_usage)
        crr_thread_cpu_usage = self.crr_cpu_usage_stats.max
        # print("Current max cpu usage: ", crr_thread_cpu_usage)
        crr_thread_cpu_usage += ssr.process_cpu_usage - old_thread_cpu_usage
        
        # print(f"crr new cpu usage max: {crr_thread_cpu_usage}, crr cpu val: {ssr.process_cpu_usage}, topic name: {triggering_topic_name}, crr cpu usage: {ssr.process_cpu_usage - old_thread_cpu_usage}, old cpu usage: {self.crr_cpu_usage_stats.max}")
        self.topic_cpu_stats[triggering_topic_name] = ssr.process_cpu_usage

        self.crr_total_elapsed_s_stats.add(ssr.total_elapsed)
        self.crr_data_read_ns_stats.add(ssr.data_read_time)
        self.crr_data_write_ns_stats.add(ssr.data_write_time)
        self.crr_data_delete_ns_stats.add(ssr.data_delete_time)
        self.crr_task_execution_ns_stats.add(ssr.task_execution_time)
        self.crr_in_data_byte_stats.add(ssr.in_data_byte)
        self.crr_out_data_byte_stats.add(ssr.out_data_byte)
        self.crr_execution_elapsed_ms_stats.add(ssr.execution_elapsed_ms)
        self.crr_current_bandwdith_usage_stats.add(ssr.current_bandwdith_usage)
        self.crr_in_local_data_byte_stats.add(ssr.in_local_data_byte)
        self.crr_in_remote_data_byte_stats.add(ssr.in_remote_data_byte)
        self.crr_out_local_data_byte_stats.add(ssr.out_local_data_byte)
        self.crr_out_remote_data_byte_stats.add(ssr.out_remote_data_byte)
        crr_added_cpu_val = self.crr_cpu_usage_stats.increment(crr_thread_cpu_usage)
        self.crr_memory_usage_stats.add(ssr.process_memory_usage)
        self.crr_raw_elapsed_time_from_init_ms.add(ssr.raw_elapsed_time_from_init_ms)
        self.crr_raw_elapsed_max_time_from_init_ms.add(ssr.raw_elapsed_max_time_from_init_ms)
        self.crr_raw_elapsed_min_time_from_init_ms.add(ssr.raw_elapsed_min_time_from_init_ms)
        
        self.total_elapsed_s_stats.add(ssr.total_elapsed)
        self.data_read_ns_stats.add(ssr.data_read_time)
        self.data_write_ns_stats.add(ssr.data_write_time)
        self.data_delete_ns_stats.add(ssr.data_delete_time)
        self.task_execution_ns_stats.add(ssr.task_execution_time)
        self.in_data_byte_stats.add(ssr.in_data_byte)
        self.out_data_byte_stats.add(ssr.out_data_byte)
        self.execution_elapsed_ms_stats.add(ssr.execution_elapsed_ms)
        self.current_bandwdith_usage_stats.add(ssr.current_bandwdith_usage)
        self.in_local_data_byte_stats.add(ssr.in_local_data_byte)
        self.in_remote_data_byte_stats.add(ssr.in_remote_data_byte)
        self.out_local_data_byte_stats.add(ssr.out_local_data_byte)
        self.out_remote_data_byte_stats.add(ssr.out_remote_data_byte)
        self.cpu_usage_stats.add(crr_added_cpu_val)
        self.memory_usage_stats.add(ssr.process_memory_usage)
        self.total_raw_elapsed_time_from_init_ms.add(ssr.raw_elapsed_time_from_init_ms)
        self.total_raw_elapsed_max_time_from_init_ms.add(ssr.raw_elapsed_max_time_from_init_ms)
        self.total_raw_elapsed_min_time_from_init_ms.add(ssr.raw_elapsed_min_time_from_init_ms)
