from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.statistics_records import StatisticRecord


class StatChecker:
    def __init__(self) -> None:
        pass
    
    def get_task_output_size_from_host_topic(self, host, action, node_records:dict):
        for host_name, record in node_records.items():
            if host_name == host and record:
                resource_consumption:ResourceUsageRecord = record
                for stat_record in resource_consumption.statistic_records:
                    if stat_record.action_name == action:
                        return stat_record.crr_out_data_byte_stats.avg
    
    # For each action the cumulative data and total executions of that action
    # can give us its performance until this moment. We can also predict how
    # that action will affect the device where it will be hosted.
    def get_current_output_cost_of_actions(self, node_records:dict):
        action_costs = {}
        action_costs_count = {}
        for _, record in node_records.items():
            resource_consumption:ResourceUsageRecord = record
            stat_record:StatisticRecord
            for stat_record in resource_consumption.statistic_records:
                if stat_record.action_name in action_costs:
                    action_costs[stat_record.action_name] += stat_record.out_data_byte_stats.total
                    action_costs_count[stat_record.action_name] += stat_record.out_data_byte_stats.count
                else:
                    action_costs[stat_record.action_name] = stat_record.out_data_byte_stats.total
                    action_costs_count[stat_record.action_name] = stat_record.out_data_byte_stats.count
        
        for cost_key in action_costs.keys():
            if action_costs[cost_key] > 0.0:
                action_costs[cost_key] /= action_costs_count[cost_key]
        
        return action_costs
    
    # For each action the cumulative data and total executions of that action
    # can give us its performance until this moment. We can also predict how
    # that action will affect the device where it will be hosted.
    def get_current_input_cost_of_actions(self, node_records:dict):
        action_costs = {}
        action_costs_count = {}
        for _, record in node_records.items():
            resource_consumption:ResourceUsageRecord = record
            stat_record:StatisticRecord
            for stat_record in resource_consumption.statistic_records:
                if stat_record.action_name in action_costs:
                    action_costs[stat_record.action_name] += stat_record.in_data_byte_stats.total
                    action_costs_count[stat_record.action_name] += stat_record.in_data_byte_stats.count
                else:
                    action_costs[stat_record.action_name] = stat_record.in_data_byte_stats.total
                    action_costs_count[stat_record.action_name] = stat_record.in_data_byte_stats.count
        
        for cost_key in action_costs.keys():
            if action_costs[cost_key] > 0.0:
                action_costs[cost_key] /= action_costs_count[cost_key]
        
        return action_costs