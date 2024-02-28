from datetime import datetime
from typing import List

from cep_library.cep.model.cep_task import CEPTask
from cep_library.management.model.statistics_records import IndividualTopicStatistics, StatisticRecord


class TaskRecords:
    # job_id: int
    # execution_count: int
    # last_elapsed_ms: float
    # active: bool
    def __init__(self):
        self.cep_task: CEPTask


class ResourceUsageRecord:
    def __init__(self):
        self.host: str
        self.ram: float
        self.cpu: float
        self.executed_task_count: int
        # executed_flow_count: int
        self.processed_msg_count: int
        self.record_date: datetime
        self.task_records: List[TaskRecords]
        self.statistic_records: List[StatisticRecord]
        self.carried_data_count: int
        self.deleted_data_count: int
        self.crr_execution_exists: bool

    def get_server_stat_header(self):
        return [
            "current_server_time_utc",
            "cep_evaluation_time_ns",
            "algorithm_eval_time_ms"
        ]

    def get_resource_usage_head(sefl):
        return [
            "id",
            "date",
            "host",
            "cpu",
            "ram"
        ]

    def get_device_stat_header(self):
        return [
            "record_id",
            "host",
            "ram",
            "cpu",
            "executed_task_count",
            "processed_msg_count",
            "record_date",
            "carried_data_count",
            "deleted_data_count",
            "current_server_time"
        ]

    def get_device_stats(self, record_id: int):
        return [
            str(record_id),
            str(self.host),
            str(self.ram),
            str(self.cpu),
            str(self.executed_task_count),
            str(self.processed_msg_count),
            str(self.record_date),
            str(self.carried_data_count),
            str(self.deleted_data_count)
        ]

    def get_task_topic_stats_file_header(self):
        return [
            "record_id",
            "host",
            "record_date",
            
            "action_name",
            "topic_name",
            
            "topics_override_count",
            "topics_new_count",
            "topics_dead_on_arrival_count",
            "topics_old_arrival_count",
            "topics_other_arrival_count",
            
            "hit_count",
            "miss_count",
            
            "current_server_time"
        ]

    def get_task_topics_stats(self, record_id: int):
        results = []
        
        stat_record: StatisticRecord = None
        for stat_record in self.statistic_records:
            topic_stat_record: IndividualTopicStatistics
            for _, topic_stat_record in stat_record.individual_topic_stats.items():
                results.append([
                    str(record_id),
                    str(self.host),
                    str(self.record_date),
                    str(stat_record.action_name),
                    str(topic_stat_record.topic_name),
                    
                    str(topic_stat_record.topics_override_count),
                    str(topic_stat_record.topics_new_count),
                    str(topic_stat_record.topics_dead_on_arrival_count),
                    str(topic_stat_record.topics_old_arrival_count),
                    str(topic_stat_record.topics_other_arrival_count),
                    
                    str(topic_stat_record.hit_count),
                    str(topic_stat_record.miss_count)
                ])
        return results

    def get_task_stats_header(self):
        return [
            "record_id",
            "host",
            "record_date",
            "action_name",
            "activation_time_ns",
            "current_record_count",
            "total_record_count",
            
            "avg_total_elapsed_s_stats",
            "avg_data_read_ns_stats",
            "avg_data_write_ns_stats",
            "avg_data_delete_ns_stats",
            "avg_task_execution_ns_stats",
            "avg_in_data_byte_stats",
            "avg_out_data_byte_stats",
            "avg_execution_elapsed_ms_stats",
            "avg_current_bandwdith_usage_stats",
            "avg_in_local_data_byte_stats",
            "avg_in_remote_data_byte_stats",
            "avg_out_local_data_byte_stats",
            "avg_out_remote_data_byte_stats",
            "avg_cpu_usage_stats",
            "avg_memory_usage_stats",
            "avg_raw_elapsed_time_from_init_ms",
            "avg_raw_elapsed_max_time_from_init_ms",
            "avg_raw_elapsed_min_time_from_init_ms",
            
            "crr_total_elapsed_s_stats",
            "crr_data_read_ns_stats",
            "crr_data_write_ns_stats",
            "crr_data_delete_ns_stats",
            "crr_task_execution_ns_stats",
            "crr_in_data_byte_stats",
            "crr_out_data_byte_stats",
            "crr_execution_elapsed_ms_stats",
            "crr_current_bandwdith_usage_stats",
            "crr_in_local_data_byte_stats",
            "crr_in_remote_data_byte_stats",
            "crr_out_local_data_byte_stats",
            "crr_out_remote_data_byte_stats",
            "crr_cpu_usage_stats",
            "crr_memory_usage_stats",
            "crr_raw_elapsed_time_from_init_ms",
            "crr_raw_elapsed_max_time_from_init_ms",
            "crr_raw_elapsed_min_time_from_init_ms",
            
            "cumulative_total_elapsed_s_stats",
            "cumulative_data_read_ns_stats",
            "cumulative_data_write_ns_stats",
            "cumulative_data_delete_ns_stats",
            "cumulative_task_execution_ns_stats",
            "cumulative_in_data_byte_stats",
            "cumulative_out_data_byte_stats",
            "cumulative_execution_elapsed_ms_stats",
            "cumulative_current_bandwdith_usage_stats",
            "cumulative_in_local_data_byte_stats",
            "cumulative_in_remote_data_byte_stats",
            "cumulative_out_local_data_byte_stats",
            "cumulative_out_remote_data_byte_stats",
            "cumulative_cpu_usage_stats",
            "cumulative_memory_usage_stats",
            "cumulative_raw_elapsed_time_from_init_ms",
            "cumulative_raw_elapsed_max_time_from_init_ms",
            "cumulative_raw_elapsed_min_time_from_init_ms",
            
            "std_total_elapsed_s_stats",
            "std_data_read_ns_stats",
            "std_data_write_ns_stats",
            "std_data_delete_ns_stats",
            "std_task_execution_ns_stats",
            "std_in_data_byte_stats",
            "std_out_data_byte_stats",
            "std_execution_elapsed_ms_stats",
            "std_current_bandwdith_usage_stats",
            "std_in_local_data_byte_stats",
            "std_in_remote_data_byte_stats",
            "std_out_local_data_byte_stats",
            "std_out_remote_data_byte_stats",
            "std_cpu_usage_stats",
            "std_memory_usage_stats",
            "std_raw_elapsed_time_from_init_ms",
            "std_raw_elapsed_max_time_from_init_ms",
            "std_raw_elapsed_min_time_from_init_ms",
            
            "hit_count",
            "miss_count",
            
            "current_server_time",
        ]

    def get_task_stats(self, record_id: int):
        result = []
        stat_record: StatisticRecord = None
        for stat_record in self.statistic_records:
            result.append(
                [
                    str(record_id),
                    str(self.host),
                    str(self.record_date),
                    str(stat_record.action_name),
                    str(stat_record.activation_time_ns),
                    str(stat_record.crr_cpu_usage_stats.count),
                    str(stat_record.cpu_usage_stats.count),
                    
                    str(stat_record.total_elapsed_s_stats.avg),                    
                    str(stat_record.data_read_ns_stats.avg),                    
                    str(stat_record.data_write_ns_stats.avg),                    
                    str(stat_record.data_delete_ns_stats.avg),                    
                    str(stat_record.task_execution_ns_stats.avg),                    
                    str(stat_record.in_data_byte_stats.avg),                    
                    str(stat_record.out_data_byte_stats.avg),                    
                    str(stat_record.execution_elapsed_ms_stats.avg),                    
                    str(stat_record.current_bandwdith_usage_stats.avg),                    
                    str(stat_record.in_local_data_byte_stats.avg),                    
                    str(stat_record.in_remote_data_byte_stats.avg),                    
                    str(stat_record.out_local_data_byte_stats.avg),                    
                    str(stat_record.out_remote_data_byte_stats.avg),                    
                    str(stat_record.cpu_usage_stats.avg),                    
                    str(stat_record.memory_usage_stats.avg),                    
                    str(stat_record.total_raw_elapsed_time_from_init_ms.avg),
                    str(stat_record.total_raw_elapsed_max_time_from_init_ms.avg),
                    str(stat_record.total_raw_elapsed_min_time_from_init_ms.avg),
                    
                    str(stat_record.crr_total_elapsed_s_stats.total),
                    str(stat_record.crr_data_read_ns_stats.total),
                    str(stat_record.crr_data_write_ns_stats.total),
                    str(stat_record.crr_data_delete_ns_stats.total),
                    str(stat_record.crr_task_execution_ns_stats.total),
                    str(stat_record.crr_in_data_byte_stats.total),
                    str(stat_record.crr_out_data_byte_stats.total),
                    str(stat_record.crr_execution_elapsed_ms_stats.total),
                    str(stat_record.crr_current_bandwdith_usage_stats.total),
                    str(stat_record.crr_in_local_data_byte_stats.total),
                    str(stat_record.crr_in_remote_data_byte_stats.total),
                    str(stat_record.crr_out_local_data_byte_stats.total),
                    str(stat_record.crr_out_remote_data_byte_stats.total),
                    str(stat_record.crr_cpu_usage_stats.total),
                    str(stat_record.crr_memory_usage_stats.total),
                    str(stat_record.crr_raw_elapsed_time_from_init_ms.total),
                    str(stat_record.crr_raw_elapsed_max_time_from_init_ms.total),
                    str(stat_record.crr_raw_elapsed_min_time_from_init_ms.total),                    
                    
                    str(stat_record.total_elapsed_s_stats.total),
                    str(stat_record.data_read_ns_stats.total),
                    str(stat_record.data_write_ns_stats.total),
                    str(stat_record.data_delete_ns_stats.total),
                    str(stat_record.task_execution_ns_stats.total),
                    str(stat_record.in_data_byte_stats.total),
                    str(stat_record.out_data_byte_stats.total),
                    str(stat_record.execution_elapsed_ms_stats.total),
                    str(stat_record.current_bandwdith_usage_stats.total),
                    str(stat_record.in_local_data_byte_stats.total),
                    str(stat_record.in_remote_data_byte_stats.total),
                    str(stat_record.out_local_data_byte_stats.total),
                    str(stat_record.out_remote_data_byte_stats.total),
                    str(stat_record.cpu_usage_stats.total),
                    str(stat_record.memory_usage_stats.total),
                    str(stat_record.total_raw_elapsed_time_from_init_ms.total),
                    str(stat_record.total_raw_elapsed_max_time_from_init_ms.total),
                    str(stat_record.total_raw_elapsed_min_time_from_init_ms.total),                    
                    
                    str(stat_record.total_elapsed_s_stats.std),
                    str(stat_record.data_read_ns_stats.std),
                    str(stat_record.data_write_ns_stats.std),
                    str(stat_record.data_delete_ns_stats.std),
                    str(stat_record.task_execution_ns_stats.std),
                    str(stat_record.in_data_byte_stats.std),
                    str(stat_record.out_data_byte_stats.std),
                    str(stat_record.execution_elapsed_ms_stats.std),
                    str(stat_record.current_bandwdith_usage_stats.std),
                    str(stat_record.in_local_data_byte_stats.std),
                    str(stat_record.in_remote_data_byte_stats.std),
                    str(stat_record.out_local_data_byte_stats.std),
                    str(stat_record.out_remote_data_byte_stats.std),
                    str(stat_record.cpu_usage_stats.std),
                    str(stat_record.memory_usage_stats.std),
                    str(stat_record.total_raw_elapsed_time_from_init_ms.std),
                    str(stat_record.total_raw_elapsed_max_time_from_init_ms.std),
                    str(stat_record.total_raw_elapsed_min_time_from_init_ms.std),                
                
                    str(stat_record.hit_count),
                    str(stat_record.miss_count),
                ]
            )
        return result
