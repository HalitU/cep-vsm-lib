import math
from copy import deepcopy
from typing import List

import numpy as np
from ortools.sat.python import cp_model

from cep_library import configs
from cep_library.cep.model.cep_task import (
    CEPTask,
    RequiredInputTopics,
    RequiredOutputTopics,
)
from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.statistics_records import (
    StatisticRecord,
    StatisticValue,
)
from cep_library.management.model.task_alteration import (
    DataMigrationModel,
    TaskAlterationModel,
)
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.raw.model.raw_settings import (
    OutputTopics,
    RawSettings,
    RawStatisticsBook,
)
from cep_library.raw.model.raw_update_event import RawUpdateEvent


class TopicKeeper:
    def __init__(self) -> None:
        self.count = 0.0
        self.duration = 0.0
        self.size = 0.0


class CP_PIPE_INT_VARS:
    def __init__(
        self,
        workers,
        topology: Topology,
        distribution_history,
        statistic_records,
        producer_records,
        last_min_cost,
        valid_stats_count,
    ) -> None:
        self.topology = topology
        self.model: cp_model.CpModel = cp_model.CpModel()
        self.distribution_history = distribution_history
        self.producer_records: dict = producer_records

        # Skip distribution if not all event stats can be collected yet
        self.node_records: dict = deepcopy(statistic_records)
        triggered_events = []
        for _, record in self.node_records.items():
            resource_consumption: ResourceUsageRecord = record
            stat: StatisticRecord
            for stat in resource_consumption.statistic_records:
                if (
                    stat.task_execution_ns_stats.count > 0
                    and stat.action_name not in triggered_events
                    and stat.crr_out_data_byte_stats.count > 0
                ):
                    all_in_exists = True
                    sval: StatisticValue
                    for (
                        _,
                        sval,
                    ) in stat.in_topic_data_size_stats.items():
                        if sval.count == 0:
                            all_in_exists = False
                            break
                    if all_in_exists == False:
                        break
                    triggered_events.append(stat.action_name)

        self.minimum_objective_cost = last_min_cost
        self.valid_stats_count = valid_stats_count

        executor_event_count = len(topology.get_executor_nodes())
        if configs.REPEAT_ON_NON_STAT == 0 and distribution_history and len(triggered_events) != executor_event_count:
            print(
                f"Not all events are triggered yet. Triggered event count: {triggered_events}, expected count: {executor_event_count}"
            )
            self.execution_exists = False
            return
        # 12/17/2023 If this is the case, then optimization should use default values to redistribute again.
        # otherwise it will get stuck until data production changes.
        elif configs.REPEAT_ON_NON_STAT == 1 and distribution_history and len(triggered_events) != executor_event_count:
            self.distribution_history = None
            print(
                f"Not all events are triggered yet. So algorithm will attempt to distribute from scratch. Triggered event count: {triggered_events}, expected count: {executor_event_count}"
            )

        self.events_name_list = triggered_events
        self.execution_exists = True

        # deepcopy of workers and statistics in case of manipulation
        self.workers: dict = deepcopy(workers)
        self.worker_keys = list(self.workers.keys())

        # sort the worker according to keys
        self.worker_keys.sort()

        # product keys
        self.product_keys = list(self.producer_records.keys())
        self.product_keys.sort()

        self.remote_cost_multiplier = configs.DIFFERENT_DEVICE_READ_PENATLY
        self.device_change_multiplier = configs.DEVICE_CHANGE_PENALTY
        self.device_non_change_multiplier = configs.DEVICE_NON_CHANGE_MULTIPLIER
        self.keep_lowest_cost_after_dist = True

        self.paths = self.topology.get_all_simple_paths()

    def run(self):
        # validation -- done
        if self.execution_exists == False:
            print("No execution statistic available yet...")
            return [], [], self.distribution_history, self.minimum_objective_cost
        # stats -- done
        (
            self.task_execution_costs,
            self.topic_data_stats,
            self.task_output_costs,
            self.cpu_usages,
        ) = self.prepare_statistics()
        # vars -- done
        self.producers, self.events = self.prepare_variables()
        # additional values
        configs.UTILIZE_PREFERRED_ACTION_LIMIT = self.calculate_maximum_preferred_cost()
        # constraints - done
        self.constraints()
        # func
        self.prepare_objective_function()
        # optimize -- done
        solver = self.optimize()
        # distribution -- done
        return self.determine_distribution(solver)

    def constraints(self):
        if configs.DEVICE_ACTION_LIMIT > 0:
            print("------------------------")
            print(f"Setting up constraints; Device action limit is: {configs.DEVICE_ACTION_LIMIT}")
            
            # define N number of variables for each worker
            for w_ix in range(1, len(self.worker_keys) + 1):
                print(f"Processing constraint for worker: {w_ix}")
                worker_sum = []
                for o_key in self.events:            
                    exec_var:cp_model.IntVar = self.events[o_key]["execution_var"]
                    
                    worker_bool_var = self.model.NewBoolVar(str(w_ix) + "_" + o_key)
                    
                    self.model.Add(exec_var == w_ix).OnlyEnforceIf(worker_bool_var)
                    self.model.Add(exec_var != w_ix).OnlyEnforceIf(worker_bool_var.Not())
                    
                    worker_sum.append(worker_bool_var)                
                    
                worker_n_events = sum(worker_sum)
                self.model.Add(worker_n_events <= configs.DEVICE_ACTION_LIMIT)    
            print("------------------------")            

        if configs.UTILIZE_PREFERRED_ACTION_COST == 1:
            print("------------------------")
            print(f"Setting up constraints; Device preferred action cost limit is: {configs.UTILIZE_PREFERRED_ACTION_LIMIT}")
            
            # define N number of variables for each worker
            for w_ix in range(1, len(self.worker_keys) + 1):
                print(f"Processing constraint for worker: {w_ix}")
                worker_preferred_sum = []
                for o_key in self.events:            
                    exec_var:cp_model.IntVar = self.events[o_key]["execution_var"]
                    preferred_cost_var:cp_model.IntVar = self.model.NewIntVar(0, cp_model.INT32_MAX, f"pref_cost_var_{w_ix}_{o_key}")
                    
                    worker_bool_var = self.model.NewBoolVar(str(w_ix) + "_" + o_key)
                    
                    self.model.Add(exec_var == w_ix).OnlyEnforceIf(worker_bool_var)
                    self.model.Add(exec_var != w_ix).OnlyEnforceIf(worker_bool_var.Not())
                    
                    current_event_preferred_cost = self.events[o_key]["preferred_cost"]
                    self.model.Add(preferred_cost_var == current_event_preferred_cost).OnlyEnforceIf(worker_bool_var)
                    self.model.Add(preferred_cost_var == 0).OnlyEnforceIf(worker_bool_var.Not())
                    
                    worker_preferred_sum.append(preferred_cost_var)                
                    
                worker_preferred_cost = sum(worker_preferred_sum)
                self.model.Add(worker_preferred_cost <= configs.UTILIZE_PREFERRED_ACTION_LIMIT)
            print("------------------------")            

        if configs.DEVICE_ACTION_MIN_LIMIT > 0:
            print("------------------------")
            print(f"Setting up constraints; Device minimum action limit is: {configs.DEVICE_ACTION_MIN_LIMIT}")
            
            # define N number of variables for each worker
            for w_ix in range(1, len(self.worker_keys) + 1):
                print(f"Processing constraint for worker: {w_ix}")
                worker_sum = []
                for o_key in self.events:            
                    exec_var:cp_model.IntVar = self.events[o_key]["execution_var"]
                    
                    worker_bool_var = self.model.NewBoolVar(str(w_ix) + "_" + o_key)
                    
                    self.model.Add(exec_var == w_ix).OnlyEnforceIf(worker_bool_var)
                    self.model.Add(exec_var != w_ix).OnlyEnforceIf(worker_bool_var.Not())
                    
                    worker_sum.append(worker_bool_var)                
                    
                worker_n_events = sum(worker_sum)
                self.model.Add(worker_n_events >= configs.DEVICE_ACTION_MIN_LIMIT)    
            print("------------------------")

        if configs.DEVICE_INTAKE_MIN_LIMIT > 0:
            print("------------------------")
            print(f"Setting up constraints; Device minimum intake limit is: {configs.DEVICE_INTAKE_MIN_LIMIT}")
            
            # define N number of variables for each worker
            for w_ix in range(1, len(self.worker_keys) + 1):
                print(f"Processing constraint for worker: {w_ix}")
                worker_sum = []
                for o_key in self.events:            
                    exec_var:cp_model.IntVar = self.events[o_key]["output_var"]
                    
                    worker_bool_var = self.model.NewBoolVar(str(w_ix) + "_" + o_key)
                    
                    self.model.Add(exec_var == w_ix).OnlyEnforceIf(worker_bool_var)
                    self.model.Add(exec_var != w_ix).OnlyEnforceIf(worker_bool_var.Not())
                    
                    worker_sum.append(worker_bool_var)                
                    
                worker_n_events = sum(worker_sum)
                self.model.Add(worker_n_events >= configs.DEVICE_INTAKE_MIN_LIMIT)    
            print("------------------------")

    def calculate_maximum_preferred_cost(self):
        if configs.UTILIZE_PREFERRED_ACTION_COST == 0:
            return 0.0
        
        cost_matrix = np.array([0.0] * len(self.worker_keys))
        t: TopologyNode
        for t in self.topology.G.nodes:
            if t.node_type == NodeType.EXECUTER:
                node_data:CEPTask = t.node_data
                min_ix = np.argmin(cost_matrix)
                cost_matrix[min_ix] += node_data._settings.expected_task_cost
        new_max = max(cost_matrix)
        print(f"Calculated preferred action limit is :{new_max}")
        return math.ceil(new_max)

    def prepare_statistics(self):
        # Statistics for calculating costs
        task_execution_costs = {}
        task_output_costs = {}
        topic_data_stats = {}
        cpu_usages = {}

        # Prepare statistics with topics and actions
        # Here we have how much an action takes and the distributions depending on the input topics
        for _, record in self.node_records.items():
            # For this host
            if record:
                resource_consumption: ResourceUsageRecord = record
                for stat_record in resource_consumption.statistic_records:
                    if stat_record.action_name not in topic_data_stats:
                        topic_data_stats[stat_record.action_name] = {}

                    # CPU usage stats
                    if stat_record.action_name not in cpu_usages:
                        nw_cpu_tk = TopicKeeper()
                        if configs.OPT_USE_CRR_VALUES_ONLY == 0:
                            nw_cpu_tk.count = stat_record.cpu_usage_stats.count
                            nw_cpu_tk.size = stat_record.cpu_usage_stats.total
                        else:
                            nw_cpu_tk.count = stat_record.crr_cpu_usage_stats.count
                            nw_cpu_tk.size = stat_record.crr_cpu_usage_stats.total                            
                        cpu_usages[stat_record.action_name] = nw_cpu_tk
                    else:
                        crr_cpu_tk: TopicKeeper = cpu_usages[stat_record.action_name]
                        if configs.OPT_USE_CRR_VALUES_ONLY == 0:
                            crr_cpu_tk.count += stat_record.cpu_usage_stats.count
                            crr_cpu_tk.size += stat_record.cpu_usage_stats.total
                        else:
                            crr_cpu_tk.count += stat_record.crr_cpu_usage_stats.count
                            crr_cpu_tk.size += stat_record.crr_cpu_usage_stats.total

                    # Topic-wise stats
                    sval: StatisticValue
                    for (
                        topic_name,
                        sval,
                    ) in stat_record.in_topic_data_size_stats.items():
                        if topic_name not in topic_data_stats[stat_record.action_name]:
                            new_tk = TopicKeeper()
                            new_tk.count += sval.count
                            new_tk.size += sval.total
                            topic_data_stats[stat_record.action_name][
                                topic_name
                            ] = new_tk
                        else:
                            crr_tk: TopicKeeper = topic_data_stats[
                                stat_record.action_name
                            ][topic_name]
                            crr_tk.count += sval.count
                            crr_tk.size += sval.total

                    # All topics are visited in the previous step
                    for (
                        topic_name,
                        sval,
                    ) in stat_record.in_topic_data_read_time_stats.items():
                        crr_tk: TopicKeeper = topic_data_stats[stat_record.action_name][
                            topic_name
                        ]
                        crr_tk.duration += sval.total

                    # Action-wise total output stats
                    if stat_record.action_name in task_output_costs:
                        crr_tk: TopicKeeper = task_output_costs[stat_record.action_name]
                        if configs.OPT_USE_CRR_VALUES_ONLY == 0:
                            crr_tk.count += stat_record.data_write_ns_stats.count
                            crr_tk.duration += stat_record.data_write_ns_stats.total
                            crr_tk.size += stat_record.out_data_byte_stats.total
                        else:
                            crr_tk.count += stat_record.crr_data_write_ns_stats.count
                            crr_tk.duration += stat_record.crr_data_write_ns_stats.total
                            crr_tk.size += stat_record.crr_out_data_byte_stats.total
                    else:
                        new_tk = TopicKeeper()
                        if configs.OPT_USE_CRR_VALUES_ONLY == 0:
                            new_tk.count += stat_record.data_write_ns_stats.count
                            new_tk.duration += stat_record.data_write_ns_stats.total
                            new_tk.size += stat_record.out_data_byte_stats.total
                        else:
                            new_tk.count += stat_record.crr_data_write_ns_stats.count
                            new_tk.duration += stat_record.crr_data_write_ns_stats.total
                            new_tk.size += stat_record.crr_out_data_byte_stats.total
                        task_output_costs[stat_record.action_name] = new_tk

                    # Action-wise total duration spent
                    if stat_record.action_name in task_execution_costs:
                        crr_tk: TopicKeeper = task_execution_costs[
                            stat_record.action_name
                        ]
                        if configs.OPT_USE_CRR_VALUES_ONLY == 0:
                            crr_tk.count += stat_record.task_execution_ns_stats.count
                            crr_tk.duration += stat_record.task_execution_ns_stats.total
                            crr_tk.size += stat_record.in_data_byte_stats.total
                        else:
                            crr_tk.count += stat_record.crr_task_execution_ns_stats.count
                            crr_tk.duration += stat_record.crr_task_execution_ns_stats.total
                            crr_tk.size += stat_record.crr_in_data_byte_stats.total
                    else:
                        new_tk = TopicKeeper()
                        if configs.OPT_USE_CRR_VALUES_ONLY == 0:
                            new_tk.count += stat_record.task_execution_ns_stats.count
                            new_tk.duration += stat_record.task_execution_ns_stats.total
                            new_tk.size += stat_record.in_data_byte_stats.total
                        else:
                            new_tk.count += stat_record.crr_task_execution_ns_stats.count
                            new_tk.duration += stat_record.crr_task_execution_ns_stats.total
                            new_tk.size += stat_record.crr_in_data_byte_stats.total
                        task_execution_costs[stat_record.action_name] = new_tk

        return task_execution_costs, topic_data_stats, task_output_costs, cpu_usages

    def prepare_variables(self):
        producers = {}
        events = {}
        t: TopologyNode
        for t in self.topology.G.nodes:
            if t.node_type == NodeType.PRODUCER:
                producers[t.name] = {
                    "host": t.node_data.producer_name,
                    "output_var": {}
                }

            elif t.node_type == NodeType.EXECUTER:
                node_data:CEPTask = t.node_data
                print (f"Current event {node_data._settings.action_name} preferred cost is: {node_data._settings.expected_task_cost}")
                events[t.name] = {
                    "execution_var": {},
                    "no_dist_exec_hint": {},
                    "output_var": {},
                    "preferred_cost": node_data._settings.expected_task_cost
                }

        # this is considered as workers are sorted in host-ip order
        w_ix = 1
        event_cost_tracker_for_hint = {}
        for o_key in events:
            events[o_key]["execution_var"] = self.model.NewIntVar(
                1, len(self.worker_keys), "e_" + o_key
            )
            events[o_key]["output_var"] = self.model.NewIntVar(
                1, len(self.worker_keys), "o_" + o_key
            )

            if configs.ENFORCE_PRODUCTION_LOCATION == 1:
                self.model.Add(events[o_key]["execution_var"] == events[o_key]["output_var"])

            # initial hints if distribution history does not exist
            if not self.distribution_history:
                # Check the cost for this worker, cannot pass the limit event for hint
                # because model will be invalid
                if configs.UTILIZE_PREFERRED_ACTION_COST == 1:
                    crr_pref_cost = events[o_key]["preferred_cost"]
                    if w_ix in event_cost_tracker_for_hint and event_cost_tracker_for_hint[w_ix] + crr_pref_cost > configs.UTILIZE_PREFERRED_ACTION_LIMIT:
                        
                        # Skip workers until next available one is hit
                        while w_ix in event_cost_tracker_for_hint and \
                            event_cost_tracker_for_hint[w_ix] + crr_pref_cost > configs.UTILIZE_PREFERRED_ACTION_LIMIT:
                            
                            if w_ix == len(self.worker_keys):
                                w_ix = 1
                            else:
                                w_ix += 1
                                
                        event_cost_tracker_for_hint[w_ix] += crr_pref_cost
                    else:
                        if w_ix in event_cost_tracker_for_hint:
                            event_cost_tracker_for_hint[w_ix] += crr_pref_cost
                        else:
                            event_cost_tracker_for_hint[w_ix] = crr_pref_cost

                print(event_cost_tracker_for_hint)

                self.model.AddHint(events[o_key]["execution_var"], w_ix)
                self.model.AddHint(events[o_key]["output_var"], w_ix)

                events[o_key]["no_dist_exec_hint"] = w_ix

                print(f"Event {o_key}; worker hint is set as: {w_ix}")

                if w_ix == len(self.worker_keys):
                    w_ix = 1
                else:
                    w_ix += 1
            else:
                previous_worker: int = self.distribution_history["events"][
                    "execution_var"
                ][o_key]
                previous_output_target: int = self.distribution_history["events"][
                    "output_var"
                ][o_key]

                self.model.AddHint(events[o_key]["execution_var"], previous_worker)
                self.model.AddHint(events[o_key]["output_var"], previous_output_target)

        print(f"Hinted worker costs after distribution: {event_cost_tracker_for_hint}")

        # Where the outputs of the producers will be written
        for p_key in producers:
            if self.distribution_history:
                previous_worker: int = self.distribution_history["producers"][p_key]

            producers[p_key]["output_var"] = self.model.NewIntVar(
                1, len(self.worker_keys), "p_" + p_key
            )

            p_ix = self.worker_keys.index(producers[p_key]['host']) + 1
            if configs.ENFORCE_PRODUCTION_LOCATION == 1:
                self.model.Add(producers[p_key]["output_var"] == p_ix)
                print(f"production index: {p_ix}")
            else:
                # initial hints if distribution history does not exist
                if not self.distribution_history:
                    self.model.AddHint(producers[p_key]["output_var"], p_ix)
                else:
                    self.model.AddHint(producers[p_key]["output_var"], previous_worker)

        return producers, events

    def get_max_possible_cost(self):
        result = 0.0
        node_count = self.topology.get_node_count()

        if self.distribution_history:
            cost_val: TopicKeeper
            for _, cost_val in self.task_execution_costs.items():
                crr_task_cost = cost_val.duration * 1.0 / cost_val.size
                result += math.ceil(crr_task_cost * self.remote_cost_multiplier)

            cost_val: TopicKeeper
            for _, cost_val in self.topic_data_stats.items():
                crr_task_cost = cost_val.duration * 1.0 / cost_val.size
                result += math.ceil(crr_task_cost * self.remote_cost_multiplier)

            cost_val: TopicKeeper
            for _, cost_val in self.task_output_costs.items():
                crr_task_cost = cost_val.duration * 1.0 / cost_val.size
                result += math.ceil(crr_task_cost * self.remote_cost_multiplier)

        else:
            for _ in range(node_count):
                if configs.USE_CUSTOM_COSTS == 1:
                    # for reading from each topic in the existence
                    result += configs.UTILIZE_PREFERRED_ACTION_LIMIT * self.remote_cost_multiplier * len(self.events_name_list)
                    # for executing
                    result += configs.UTILIZE_PREFERRED_ACTION_LIMIT * self.remote_cost_multiplier
                    # for writing
                    result += configs.UTILIZE_PREFERRED_ACTION_LIMIT * self.remote_cost_multiplier                    
                else:
                    # for reading from each topic in the existence
                    result += 5.0 * self.remote_cost_multiplier * len(self.events_name_list)
                    # for executing
                    result += 5.0 * self.remote_cost_multiplier
                    # for writing
                    result += 5.0 * self.remote_cost_multiplier

        return math.ceil(result * node_count)

    def prepare_objective_function(self):
        # region Objective function
        print("-----------------------------------------------")
        max_possible_value = cp_model.INT32_MAX  # self.get_max_possible_cost()
        print(
            f"Creating objective function, maximum possible value is: {max_possible_value}"
        )

        path_variables = []
        path_topic_cost_vars = {}
        diff = self.model.NewIntVar(0, max_possible_value, "difference_goal")

        for path in self.paths:
            # s is a single producer node or action node identifier name
            print("--------------------------------------------")
            # print("Current optimization path: ", path)
            objective_terms = []
            step: TopologyNode
            for s_ix, step in enumerate(path):
                # producer: check if the output location of the producer and producer device is same
                if step.node_type == NodeType.PRODUCER:
                    # common variables
                    n_data: RawSettings = step.node_data
                    raw_name: str = n_data.raw_data_name
                    n_stats: RawStatisticsBook = self.producer_records[raw_name]
                    p_ix = self.product_keys.index(raw_name)

                    # if variable is already calculated no need to duplicate it!
                    var_key = f"producer_{raw_name}" 
                    if var_key in path_topic_cost_vars:
                        objective_terms.append(path_topic_cost_vars[var_key])
                        continue

                    # cost calculation
                    producer_low_cost = 0
                    producer_high_cost = 0
                    if not self.distribution_history:
                        if configs.USE_CUSTOM_COSTS == 1:
                            producer_low_cost = n_data.expected_cost
                            producer_high_cost = math.ceil(n_data.expected_cost * self.remote_cost_multiplier)
                        else:
                            producer_low_cost = 5
                            producer_high_cost = math.ceil(5 * self.remote_cost_multiplier)
                    else:
                        producer_write_cost = (
                            n_stats.write_time_ns_total
                            * 1.0
                            / n_stats.write_size_byte_total
                        )
                        previous_worker: int = self.distribution_history["producers"][
                            raw_name
                        ]
                        # if previous host was different, then original expected cost should be lower
                        if previous_worker != p_ix:
                            producer_low_cost = (
                                math.ceil(producer_write_cost / self.remote_cost_multiplier)
                            )
                            producer_high_cost = producer_write_cost
                        else:
                            producer_low_cost = producer_write_cost
                            producer_high_cost = (
                                math.ceil(producer_write_cost * self.remote_cost_multiplier)
                            )

                    # print("---")
                    # print(
                    #     f"Producer low cost: {producer_low_cost}, high cost: {producer_high_cost}"
                    # )

                    producer_low_cost = math.ceil(producer_low_cost)
                    producer_high_cost = math.ceil(producer_high_cost)

                    # objective term
                    # Add additional penalty if previous choice location is changed
                    producer_objective_cost = self.model.NewIntVar(
                        0, cp_model.INT32_MAX, step.name + "_prod_var"
                    )
                    worker_prod_var: cp_model.IntVar = self.producers[step.name][
                        "output_var"
                    ]

                    # if this is true, it means that this device was the previous host
                    # and changing hosts should incur penatly
                    if self.distribution_history:
                        prev_producer_host_var: int = self.distribution_history["producers"][step.name]
                    else:
                        prev_producer_host_var: int = p_ix

                    same_prev_device_bool_var = self.model.NewBoolVar(
                        step.name + "_prod_s_d"
                    )
                    self.model.Add(
                        worker_prod_var == prev_producer_host_var
                    ).OnlyEnforceIf(same_prev_device_bool_var)
                    self.model.Add(
                        worker_prod_var != prev_producer_host_var
                    ).OnlyEnforceIf(same_prev_device_bool_var.Not())

                    self_host_device_bool_var = self.model.NewBoolVar(
                        step.name + "_prod_self_d"
                    )
                    self.model.Add(worker_prod_var == p_ix).OnlyEnforceIf(
                        self_host_device_bool_var
                    )
                    self.model.Add(worker_prod_var != p_ix).OnlyEnforceIf(
                        self_host_device_bool_var.Not()
                    )

                    self.model.Add(
                        producer_objective_cost == (producer_low_cost * self.device_non_change_multiplier)
                    ).OnlyEnforceIf(same_prev_device_bool_var).OnlyEnforceIf(
                        self_host_device_bool_var
                    )

                    self.model.Add(
                        producer_objective_cost
                        == math.ceil(producer_low_cost * self.device_change_multiplier)
                    ).OnlyEnforceIf(same_prev_device_bool_var.Not()).OnlyEnforceIf(
                        self_host_device_bool_var
                    )

                    self.model.Add(
                        producer_objective_cost == (producer_high_cost * self.device_non_change_multiplier)
                    ).OnlyEnforceIf(same_prev_device_bool_var).OnlyEnforceIf(
                        self_host_device_bool_var.Not()
                    )

                    self.model.Add(
                        producer_objective_cost
                        == math.ceil(producer_high_cost * self.device_change_multiplier)
                    ).OnlyEnforceIf(same_prev_device_bool_var.Not()).OnlyEnforceIf(
                        self_host_device_bool_var.Not()
                    )

                    path_topic_cost_vars[var_key] = producer_objective_cost

                    objective_terms.append(producer_objective_cost)

                # action executer
                elif step.node_type == NodeType.EXECUTER:
                    # two portions, first is whether that specific code is executed at the device or not
                    # second is the data flow check
                    previous_step: TopologyNode = path[s_ix - 1]
                    step_data:CEPTask = step.node_data
                    
                    # if variable is already calculated no need to duplicate it!
                    var_key = f"executor_{previous_step.name}_{step.name}"
                    if var_key in path_topic_cost_vars:
                        objective_terms.append(path_topic_cost_vars[var_key])
                        continue                    
                    node_cost = []
                    
                    if previous_step.node_type == NodeType.PRODUCER:
                        previous_step_output_var: cp_model.IntVar = self.producers[
                            previous_step.name
                        ]["output_var"]
                        if self.distribution_history:
                            old_read_location_var: int = self.distribution_history[
                                "producers"
                            ][previous_step.name]
                    else:
                        previous_step_output_var: cp_model.IntVar = self.events[
                            previous_step.name
                        ]["output_var"]
                        if self.distribution_history:
                            old_read_location_var: int = self.distribution_history[
                                "events"
                            ]["output_var"][previous_step.name]

                    # Stats can only be calculated after first distribution period
                    if self.distribution_history:
                        prev_output_topic_name = previous_step.get_output_topic()
                        prev_output_stat: TopicKeeper = self.topic_data_stats[
                            step.name
                        ][prev_output_topic_name]

                        task_output_cost: TopicKeeper = self.task_output_costs[
                            step.name
                        ]

                        old_output_target_var: int = self.distribution_history[
                            "events"
                        ]["output_var"][step.name]
                        old_execution_var: int = self.distribution_history["events"][
                            "execution_var"
                        ][step.name]

                        #region task execution cost
                        current_input_topics = step.get_inputs_topics()
                        current_task_execution_cost: TopicKeeper = (
                            self.task_execution_costs[step.name]
                        )
                        crr_task_cost = (
                            current_task_execution_cost.duration
                            * 1.0
                            / current_task_execution_cost.size
                        )

                        # Task cost can differ according to where the data will be
                        # read from, we can find a ratio from previous runs and
                        # utilize where input/output flows from to determine a
                        # new cost according to previous execution time and processed
                        # data size. For now, lets just distribute it according to
                        # how all input topics affect it!
                        crr_topic_total_size = 0.0
                        for input_topic in current_input_topics:
                            sub_crr_topic_cost: TopicKeeper = self.topic_data_stats[
                                step.name
                            ][input_topic]
                            crr_topic_total_size += sub_crr_topic_cost.size

                        # So the part that this data playw will be equal to its size, which
                        # may not hold for all scenarios as size does not equal to functionality
                        # However, here we assume that bigger size ==> longer execution times!
                        task_execution_cost = crr_task_cost * (
                            prev_output_stat.size * 1.0 / crr_topic_total_size
                        )
                        #endregion task execution cost
                    else:
                        old_execution_var: int = self.events[step.name]["no_dist_exec_hint"]

                    current_execution_var: cp_model.IntVar = self.events[step.name][
                        "execution_var"
                    ]
                    current_output_var: cp_model.IntVar = self.events[step.name][
                        "output_var"
                    ]

                    # local variables
                    crr_multiplier_name = step.name + "_" + previous_step.name + "_"

                    # Bool vars for enforcing rules

                    # region default cost calculation
                    data_read_low_cost = None
                    data_read_high_cost = None
                    data_write_low_cost = None
                    data_write_high_cost = None
                    if not self.distribution_history:
                        if configs.USE_CUSTOM_COSTS == 1:
                            task_execution_cost = step_data._settings.expected_task_cost
                            data_read_low_cost = step_data._settings.expected_task_cost
                            data_read_high_cost = math.ceil(step_data._settings.expected_task_cost * self.remote_cost_multiplier)
                            data_write_low_cost = step_data._settings.expected_task_cost
                            data_write_high_cost = math.ceil(step_data._settings.expected_task_cost * self.remote_cost_multiplier)
                        else:
                            task_execution_cost = 5
                            data_read_low_cost = 5
                            data_read_high_cost = math.ceil(5 * self.remote_cost_multiplier)
                            data_write_low_cost = 5
                            data_write_high_cost = math.ceil(5 * self.remote_cost_multiplier)

                        data_read_low_cost = math.ceil(data_read_low_cost)
                        data_read_high_cost = math.ceil(data_read_high_cost)
                        data_write_low_cost = math.ceil(data_write_low_cost)
                        data_write_high_cost = math.ceil(data_write_high_cost)

                    task_execution_cost = math.ceil(task_execution_cost)
                    
                    # print("---")
                    # print(f"Executor costs")
                    # print(f"Task execution cost: {task_execution_cost}")
                    # print(
                    #     f"Data read low cost: {data_read_low_cost}, high cost: {data_read_high_cost}"
                    # )
                    # print(
                    #     f"Data write low cost: {data_write_low_cost}, high cost: {data_write_high_cost}"
                    # )

                    # endregion default cost calculation

                    # region Cost of executing the method in this device
                    prev_execution_device = self.model.NewBoolVar(
                        crr_multiplier_name + "_prev_exec"
                    )
                    self.model.Add(
                        current_execution_var == old_execution_var
                    ).OnlyEnforceIf(prev_execution_device)
                    self.model.Add(
                        current_execution_var != old_execution_var
                    ).OnlyEnforceIf(prev_execution_device.Not())

                    crr_task_execution_cost = self.model.NewIntVar(
                        0, max_possible_value, crr_multiplier_name + "task"
                    )

                    self.model.Add(
                        crr_task_execution_cost == (task_execution_cost * self.device_non_change_multiplier)
                    ).OnlyEnforceIf(prev_execution_device)

                    self.model.Add(
                        crr_task_execution_cost
                        == math.ceil(task_execution_cost * self.device_change_multiplier)
                    ).OnlyEnforceIf(prev_execution_device.Not())

                    node_cost.append(crr_task_execution_cost)
                    # endregion Cost of executing the method in this device

                    # region Cost of reading data
                    # does the previous step write its output to current execution device?
                    prev_step_output_device = self.model.NewBoolVar(
                        crr_multiplier_name + "_prev_step_out"
                    )
                    self.model.Add(
                        previous_step_output_var == current_execution_var
                    ).OnlyEnforceIf(prev_step_output_device)
                    self.model.Add(
                        previous_step_output_var != current_execution_var
                    ).OnlyEnforceIf(prev_step_output_device.Not())

                    crr_read_cost_variable = self.model.NewIntVar(
                        0, max_possible_value, crr_multiplier_name + "read"
                    )

                    if not self.distribution_history:
                        self.model.Add(
                            crr_read_cost_variable == data_read_low_cost
                        ).OnlyEnforceIf(prev_step_output_device)

                        self.model.Add(
                            crr_read_cost_variable == data_read_high_cost
                        ).OnlyEnforceIf(prev_step_output_device.Not())
                    else:
                        # reading cost
                        read_cost = (
                            prev_output_stat.duration * 1.0 / prev_output_stat.size
                        )
                        data_read_low_cost_var = self.model.NewIntVar(
                            0,
                            cp_model.INT32_MAX,
                            crr_multiplier_name + "data_read_low_cost_var",
                        )
                        data_read_high_cost_var = self.model.NewIntVar(
                            0,
                            cp_model.INT32_MAX,
                            crr_multiplier_name + "data_read_high_cost_var",
                        )

                        print(f"Using read cost as {read_cost}")

                        # was the previous data source this device?
                        old_read_device = self.model.NewBoolVar(
                            crr_multiplier_name + "_old_out"
                        )
                        self.model.Add(
                            old_read_location_var == current_execution_var
                        ).OnlyEnforceIf(old_read_device)
                        self.model.Add(
                            old_read_location_var != current_execution_var
                        ).OnlyEnforceIf(old_read_device.Not())

                        # low cost calculation
                        self.model.Add(
                            data_read_low_cost_var
                            == math.ceil(read_cost / self.remote_cost_multiplier)
                        ).OnlyEnforceIf(old_read_device.Not())
                        self.model.Add(
                            data_read_low_cost_var == math.ceil(read_cost)
                        ).OnlyEnforceIf(old_read_device)

                        # high cost calculation
                        self.model.Add(
                            data_read_high_cost_var == math.ceil(read_cost)
                        ).OnlyEnforceIf(old_read_device.Not())
                        self.model.Add(
                            data_read_high_cost_var
                            == math.ceil(read_cost * self.remote_cost_multiplier)
                        ).OnlyEnforceIf(old_read_device)

                        # objective functions
                        self.model.Add(
                            crr_read_cost_variable == (data_read_low_cost_var * self.device_non_change_multiplier)
                        ).OnlyEnforceIf(prev_step_output_device).OnlyEnforceIf(
                            old_read_device
                        )

                        self.model.Add(
                            crr_read_cost_variable
                            == data_read_low_cost_var * math.ceil(self.device_change_multiplier)
                        ).OnlyEnforceIf(prev_step_output_device).OnlyEnforceIf(
                            old_read_device.Not()
                        )

                        self.model.Add(
                            crr_read_cost_variable
                            == data_read_high_cost_var * math.ceil(self.device_change_multiplier)
                        ).OnlyEnforceIf(prev_step_output_device.Not()).OnlyEnforceIf(
                            old_read_device
                        )

                        # does the previous event write to same location? 
                        prev_step_old_output_device = self.model.NewBoolVar(
                            crr_multiplier_name + "_prev_old_out"
                        )
                        self.model.Add(
                            old_read_location_var == previous_step_output_var
                        ).OnlyEnforceIf(prev_step_old_output_device)
                        self.model.Add(
                            old_read_location_var != previous_step_output_var
                        ).OnlyEnforceIf(prev_step_old_output_device.Not())

                        self.model.Add(
                            crr_read_cost_variable == (data_read_high_cost_var * self.device_non_change_multiplier)
                        ).OnlyEnforceIf(prev_step_output_device.Not()).OnlyEnforceIf(
                            old_read_device.Not()
                        ).OnlyEnforceIf(
                            prev_step_old_output_device
                        )

                        self.model.Add(
                            crr_read_cost_variable
                            == data_read_high_cost_var * math.ceil(self.device_change_multiplier)
                        ).OnlyEnforceIf(prev_step_output_device.Not()).OnlyEnforceIf(
                            old_read_device.Not()
                        ).OnlyEnforceIf(
                            prev_step_old_output_device.Not()
                        )

                    node_cost.append(crr_read_cost_variable)
                    # endregion Cost of reading data

                    # region Cost of writing data
                    current_step_output_device = self.model.NewBoolVar(
                        crr_multiplier_name + "_prev_step_out"
                    )
                    self.model.Add(
                        current_output_var == current_execution_var
                    ).OnlyEnforceIf(current_step_output_device)
                    self.model.Add(
                        current_output_var != current_execution_var
                    ).OnlyEnforceIf(current_step_output_device.Not())

                    crr_write_cost_variable = self.model.NewIntVar(
                        0, max_possible_value, crr_multiplier_name + "write"
                    )

                    if not self.distribution_history:
                        self.model.Add(
                            crr_write_cost_variable == data_write_low_cost
                        ).OnlyEnforceIf(current_step_output_device)

                        self.model.Add(
                            crr_write_cost_variable == data_write_high_cost
                        ).OnlyEnforceIf(current_step_output_device.Not())
                    else:
                        old_write_device = self.model.NewBoolVar(
                            crr_multiplier_name + "_old_out"
                        )
                        self.model.Add(
                            old_output_target_var == current_execution_var
                        ).OnlyEnforceIf(old_write_device)
                        self.model.Add(
                            old_output_target_var != current_execution_var
                        ).OnlyEnforceIf(old_write_device.Not())

                        # writing cost
                        write_cost = (
                            task_output_cost.duration * 1.0 / task_output_cost.size
                        )
                        data_write_low_cost_var = self.model.NewIntVar(
                            0,
                            cp_model.INT32_MAX,
                            crr_multiplier_name + "data_write_low_cost_var",
                        )
                        data_write_high_cost_var = self.model.NewIntVar(
                            0,
                            cp_model.INT32_MAX,
                            crr_multiplier_name + "data_write_high_cost_var",
                        )

                        print(f"Using write cost as {write_cost}")

                        self.model.Add(
                            data_write_low_cost_var
                            == math.ceil(write_cost / self.remote_cost_multiplier)
                        ).OnlyEnforceIf(old_write_device.Not())
                        self.model.Add(
                            data_write_low_cost_var == math.ceil(write_cost)
                        ).OnlyEnforceIf(old_write_device)

                        self.model.Add(
                            data_write_high_cost_var == math.ceil(write_cost)
                        ).OnlyEnforceIf(old_write_device.Not())
                        self.model.Add(
                            data_write_high_cost_var
                            == math.ceil(write_cost * self.remote_cost_multiplier)
                        ).OnlyEnforceIf(old_write_device)

                        self.model.Add(
                            crr_write_cost_variable == (data_write_low_cost_var * self.device_non_change_multiplier)
                        ).OnlyEnforceIf(current_step_output_device).OnlyEnforceIf(
                            old_write_device
                        )

                        self.model.Add(
                            crr_write_cost_variable
                            == data_write_low_cost_var * math.ceil(self.device_change_multiplier)
                        ).OnlyEnforceIf(current_step_output_device).OnlyEnforceIf(
                            old_write_device.Not()
                        )

                        self.model.Add(
                            crr_write_cost_variable
                            == data_write_high_cost_var * math.ceil(self.device_change_multiplier)
                        ).OnlyEnforceIf(current_step_output_device.Not()).OnlyEnforceIf(
                            old_write_device
                        )

                        # getting the output variable for this event from previously
                        # written host
                        old_output_device_same_var = self.model.NewBoolVar(
                            crr_multiplier_name + "_prev_old_out"
                        )
                        self.model.Add(
                            old_output_target_var == current_output_var
                        ).OnlyEnforceIf(old_output_device_same_var)
                        self.model.Add(
                            old_output_target_var != current_output_var
                        ).OnlyEnforceIf(old_output_device_same_var.Not())

                        self.model.Add(
                            crr_write_cost_variable
                            == data_write_high_cost_var * math.ceil(self.device_change_multiplier)
                        ).OnlyEnforceIf(current_step_output_device.Not()).OnlyEnforceIf(
                            old_write_device.Not()
                        ).OnlyEnforceIf(
                            old_output_device_same_var.Not()
                        )

                        self.model.Add(
                            crr_write_cost_variable == (data_write_high_cost_var * self.device_non_change_multiplier)
                        ).OnlyEnforceIf(current_step_output_device.Not()).OnlyEnforceIf(
                            old_write_device.Not()
                        ).OnlyEnforceIf(
                            old_output_device_same_var
                        )

                    # endregion Cost of writing data
                    
                    node_cost.append(crr_write_cost_variable)
                    path_topic_cost_vars[var_key] = sum(node_cost)
                    
                    objective_terms.append(path_topic_cost_vars[var_key])

            summed_term = sum(objective_terms)
            # print("Sum term: ", summed_term)
            # path_variables.append(summed_term)
            self.model.Add(diff >= summed_term)

        # abs_equation = 0.0
        # term_size = len(path_variables)
        # for t_ix in range(term_size - 1):
        #     for s_ix in range(t_ix + 1, term_size):
        #         path_term_main = path_variables[t_ix]
        #         path_term_sub = path_variables[s_ix]
        #         abs_var = self.model.NewIntVar(0, max_possible_value, f"Diff")
        #         self.model.AddAbsEquality(abs_var, path_term_main - path_term_sub)
        #         abs_equation += abs_var

        # model.AddAbsEquality(diff, abs_equation)
        self.model.Minimize(
            diff * configs.DIFFERENCE_BALANCE_WEIGHT
            # + abs_equation * configs.DIFFERENCE_EQUALIZER_WEIGHT
        )

        # endregion Objective function

    def optimize(self):
        # region Solving the problem

        print("Running the optimization...")

        def cp_logger(log):
            print(log)

        # Invoke the solver
        solution_found = False
        solver = cp_model.CpSolver()
        solver.parameters.num_workers = 1
        solver.parameters.max_time_in_seconds = configs.CP_RUNTIME_SECONDS

        if configs.MANIPULATE_CP_PARAMS == 1:
            solver.parameters.linearization_level = 0
            # solver.parameters.share_objective_bounds = True
            # solver.parameters.share_level_zero_bounds = True
            # solver.parameters.solution_pool_size = 20
            solver.parameters.cp_model_presolve = True
            solver.parameters.cp_model_probing_level = 0
            solver.parameters.log_search_progress = True
        solver.log_callback = cp_logger
        status = solver.Solve(self.model)
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            print(f"Status OPTIMAL: {status == cp_model.OPTIMAL}")
            print(f"Status FEASIBLE: {status == cp_model.FEASIBLE}")
            print(f"Solution found: {solver.ObjectiveValue()}")
            print("--------------------------------")
            print("Event execution distribution: ")
            print(
                "\n".join(
                    [
                        str(solver.Value(self.events[event]["execution_var"]))
                        for event in self.events
                    ]
                )
            )
            print("--------------------------------")
            print("Event output location distribution: ")
            print(
                "\n".join(
                    [
                        str(solver.Value(self.events[event]["output_var"]))
                        for event in self.events
                    ]
                )
            )
            print("--------------------------------")
            print("Raw data producer output location distribution: ")
            print(
                "\n".join(
                    [
                        str(solver.Value(self.producers[producer]["output_var"]))
                        for producer in self.producers
                    ]
                )
            )
            solution_found = True
        else:
            print(f"Status INFEASIBLE: {status == cp_model.INFEASIBLE}")
            print(f"Status MODEL_INVALID: {status == cp_model.MODEL_INVALID}")
            print(f"Status UNKNOWN: {status == cp_model.UNKNOWN}")
            print("No solution found.")

        if solution_found == False:
            raise Exception("No solution found.")
        else:
            print(f"Optimized solution is: {solver.ObjectiveValue()}")
        # endregion Solving
        return solver

    def determine_distribution(self, solver: cp_model.CpSolver):
        if self.distribution_history and self.cpu_usages:
            print(f"Current valid stats count: {self.valid_stats_count}")
            if (
                configs.MIN_COST_AFTER_N_DIST != -1
                and self.valid_stats_count > configs.MIN_COST_AFTER_N_DIST
            ):
                if solver.ObjectiveValue() > self.minimum_objective_cost:
                    print("No need to update distribution since objective is higher")
                    print(
                        f"Mimimum cost: {self.minimum_objective_cost}, Currently found: {solver.ObjectiveValue()}"
                    )
                    return (
                        [],
                        [],
                        self.distribution_history,
                        self.minimum_objective_cost,
                    )
                else:
                    print("Updating minimum found objective cost...")
                    print(
                        f"Mimimum cost: {self.minimum_objective_cost}, Currently found: {solver.ObjectiveValue()}"
                    )
                    self.minimum_objective_cost = solver.ObjectiveValue()

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []

        # region distribution
        # BFS from the root node and assign where data is stored as well as where task will be executed
        print("Traversing the tree with BFS algorithm...")
        new_distribution_history = {
            "events": {"execution_var": {}, "output_var": {}},
            "producers": {},
        }
        bfs_tree = self.topology.get_topological_ordered_nodes()
        node: TopologyNode
        n_raw_manipulation_created = 0
        n_tasks_activated = 0
        n_tasks_deactivated = 0
        for node in bfs_tree:
            if node.node_type == NodeType.PRODUCER:
                # predecessor data and where it writes its output
                prod_data: RawSettings = node.node_data

                producer_target_host_ix = solver.Value(
                    self.producers[prod_data.raw_data_name]["output_var"]
                )
                producer_target_host = self.worker_keys[producer_target_host_ix - 1]

                # Keeping the history
                new_distribution_history["producers"][
                    prod_data.raw_data_name
                ] = producer_target_host_ix

                # producer update request
                output_topics = []
                pst: OutputTopics
                for pst in prod_data.output_topics:
                    pst.target_database = producer_target_host
                    output_topics.append(pst.output_topic)
                producer_target_db_update = RawUpdateEvent()
                producer_target_db_update.producer_name = prod_data.producer_name
                producer_target_db_update.output_topics = prod_data.output_topics
                producer_updates.append(producer_target_db_update)

                n_raw_manipulation_created = n_raw_manipulation_created + 1

                # Update the successor required input topics
                successors = self.topology.get_successors(node)
                succ: TopologyNode
                for succ in successors:
                    # Another raw producer cannot be a successor
                    if succ.node_type == NodeType.EXECUTER:
                        succ_data: CEPTask = succ.node_data
                        input_topic: RequiredInputTopics
                        for input_topic in succ_data._settings.required_sub_tasks:
                            if input_topic.input_topic in output_topics:
                                input_topic.stored_database = producer_target_host

            # Rest are executors and the final sink which is redundant
            # Since immediate executors are processed previously we need to update
            # the preferred databases of the previous nodes if round-robin requires us to do so
            if node.node_type == NodeType.EXECUTER:
                # Task info
                task: CEPTask = node.node_data
                current_action_name = task._settings.action_name

                execution_host_ix = solver.Value(
                    self.events[current_action_name]["execution_var"]
                )
                output_host_ix = solver.Value(
                    self.events[current_action_name]["output_var"]
                )
                execution_host = self.worker_keys[execution_host_ix - 1]
                output_host = self.worker_keys[output_host_ix - 1]

                # Keeping the history
                new_distribution_history["events"]["execution_var"][
                    task._settings.action_name
                ] = execution_host_ix
                new_distribution_history["events"]["output_var"][
                    task._settings.action_name
                ] = output_host_ix

                # where the code will be executed
                task._settings.host_name = execution_host

                output_topics = []
                output_topic: RequiredOutputTopics
                for output_topic in task._settings.output_topics:
                    output_topic.target_database = output_host
                    output_topics.append(output_topic.output_topic)

                # Update the successors
                # Update the successor required input topics
                successors = self.topology.get_successors(node)
                succ: TopologyNode
                for succ in successors:
                    if succ.node_type == NodeType.EXECUTER:
                        # Another raw producer cannot be a successor
                        succ_data: CEPTask = succ.node_data
                        input_topic: RequiredInputTopics
                        for input_topic in succ_data._settings.required_sub_tasks:
                            if input_topic.input_topic in output_topics:
                                input_topic.stored_database = output_host

                # Alteration requests
                alteration = TaskAlterationModel()
                alteration.host = task._settings.host_name
                alteration.activate = True
                alteration.job_name = task._settings.action_name
                alteration.cep_task = task
                alterations.append(alteration)
                n_tasks_activated += 1

                # for non chosen hosts this task should be deactivated
                for host in [*self.workers]:
                    if host != task._settings.host_name:
                        # assumes that only one output topic is written/being published to
                        migration_request = DataMigrationModel()
                        migration_request.topic = task._settings.output_topics[
                            0
                        ].output_topic
                        migration_request.current_host = host
                        migration_request.target_host = task._settings.host_name

                        deactivation = TaskAlterationModel()
                        deactivation.host = host
                        deactivation.activate = False
                        deactivation.job_name = task._settings.action_name
                        deactivation.migration_requests = [migration_request]
                        alterations.append(deactivation)
                        n_tasks_deactivated += 1

        print(
            "Number of tasks activated is: ",
            n_tasks_activated,
            " deactivated: ",
            n_tasks_deactivated,
            ", Number of raw data manipulation: ",
            n_raw_manipulation_created,
        )
        print("Finalized distribution history: ", new_distribution_history)
        # endregion distribution
        return (
            alterations,
            producer_updates,
            new_distribution_history,
            self.minimum_objective_cost,
        )


# https://developers.google.com/optimization/cp/cp_solver
# gives different penalty to different latencies etc.
def constrained_programming(
    workers: dict,
    topology: Topology,
    statistic_records,
    producer_records,
    distribution_history,
    last_min_cost,
    valid_stats_count,
) -> tuple[List[TaskAlterationModel], List[RawUpdateEvent]]:
    print(
        "------------------------------------------------------------------------------"
    )
    print(
        "------------------------------------------------------------------------------"
    )
    print(
        "------------------------------------------------------------------------------"
    )
    print(
        "Running the Test Constrained Programming solver with enforcing device change..."
    )

    # If no workers are registered yet no need to go further
    if not workers or not producer_records:
        print("No workers or producers to build an optimization for.")
        return [], [], {}, last_min_cost

    pipe = CP_PIPE_INT_VARS(
        workers,
        topology,
        distribution_history,
        statistic_records,
        producer_records,
        last_min_cost,
        valid_stats_count,
    )
    return pipe.run()
