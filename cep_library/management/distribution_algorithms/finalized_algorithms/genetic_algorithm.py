from copy import deepcopy
from datetime import datetime, timezone
import math
from typing import List
from cep_library.management.distribution_algorithms.finalized_algorithms.constrained_programming import TopicKeeper
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.cep.model.cep_task import CEPTask, RequiredInputTopics


from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.statistics_records import StatisticValue
from cep_library.management.model.task_alteration import (
    DataMigrationModel,
    TaskAlterationModel,
)
from cep_library.raw.model.raw_settings import RawSettings, RawStatisticsBook
from cep_library.raw.model.raw_update_event import RawUpdateEvent
import numpy as np
import cep_library.configs as configs

class CEPGeneticAlgorithm:
    def __init__(self, workers, topology: Topology, statistic_records, producer_records, last_best_encoding) -> None:
        self.workers = workers
        self.topology = topology
        self.node_records: dict = deepcopy(statistic_records)
        self.producer_records = producer_records
        self.max_generation = 20
        self.population_size = 200
        self.parent_selection = 5
        self.mutation_size = 50
        self.mutation_probability = 0.5
        self.device_limit = configs.DEVICE_ACTION_LIMIT
        self.population = []
        self.remote_cost_multiplier = configs.DIFFERENT_DEVICE_READ_PENATLY
        self.migration_penalty = configs.DEVICE_CHANGE_PENALTY
        self.raw_output_costs:dict[str, float] = {}
        self.event_reading_costs:dict[str, float] = {}
        self.event_execution_costs:dict[str, float] = {}
        self.event_output_costs:dict[str, float] = {}
        self.task_execution_costs:dict[str, float] = {}
        self.task_reading_costs:dict[str, float] = {}
        self.task_output_costs:dict[str, float] = {}
        self.task_execution_costs, self.task_reading_costs, self.task_output_costs, _ = self.event_statistics()
        self.last_best_encoding=last_best_encoding

    def solve(self):
        # If no workers are registered yet no need to go further
        if not self.workers:
            return [], []
        
        # GA should aim to find the solution with minimum max_path_cost to increase critical path performance
        start_time = datetime.now(tz=timezone.utc)
        best_citizen_in_population = [], -1
        self.population = self.initialize_population()
        best_citizen_in_population, _ = self.update_best_solution(best_citizen_in_population, self.population)
        initial_population_elapsed = (datetime.now(tz=timezone.utc) - start_time).seconds
        print(f"Initial population took {initial_population_elapsed} seconds to generate.")
        gen_ix = 0
        for _ in range(self.max_generation):
            # Keep best solutions - x
            parents_to_keep = self.ga_select_ranking()
            
            # Cross parents - y => x + y = p
            children = self.ga_cross_single_point()
            
            # Mutate some candidates from new p
            self.ga_mutate(parents_to_keep, children)
            
            if len(self.population) != self.population_size:
                raise Exception("Newly generated population does not match the population size!")
            
            # Update best
            prev_best = best_citizen_in_population
            best_citizen_in_population, sol_changed = self.update_best_solution(best_citizen_in_population, self.population)
            
            if sol_changed and best_citizen_in_population[1] == prev_best[1]:
                raise Exception("Solution changed but model did not!")

            gen_ix += 1

        print(f"Number of generations finished: {gen_ix}")
        
        if gen_ix != self.max_generation:
            raise Exception("Number of generations does not match with what is required!")

        return self.get_final_distribution(best_citizen_in_population)
    
    def ga_select_ranking(self):
        # Rank the population according to their fitness in descending order
        self.population.sort(key=lambda t: t[1], reverse=True)
        
        # Choose N parents according to Elitism selection
        return self.population[:self.parent_selection]
    
    def update_best_solution(self, current_best_solution, population):
        sol_changed=False
        for p in population:            
            if p[1] > current_best_solution[1]:
                current_best_solution = p
                sol_changed = True
        if sol_changed:
            print("Best solution is changed.")
        return current_best_solution, sol_changed

    def ga_cross_single_point(self):
        # Choose two candidates from the current population to generate a children
        # according to their fitness value
        encodings = []
        weights = []
        enc_len = 0
        for enc_ix, (enc, w) in enumerate(self.population):
            if w != 0:
                enc_len = len(enc)
                encodings.append(enc_ix)
                weights.append(w)
            
        weights = np.divide(weights, sum(weights))
        
        # if sum(weights) != 1.0:
        #     raise Exception(f"Sum of weights must be 1.0 but it is: {sum(weights)}!")
        
        # We generate children
        new_generation = []
        for _ in range(0, self.population_size - self.parent_selection):
            current_parents = np.random.choice(encodings, size=2, p=weights, replace=False)
            
            # Cut the encoding at random spot
            first_encoding = self.population[current_parents[0]][0]
            second_encoding = self.population[current_parents[1]][0]
            
            cut_point = np.random.randint(0, len(first_encoding))
            
            new_child = first_encoding[:cut_point] + second_encoding[cut_point:]
            
            # Calculate the fitness value of the new child
            child_fitness = self.calculate_fitness(new_child)
            
            if len(new_child) != enc_len:
                raise Exception("Invalid child encoding size detected.")
            
            # Finished generating a new child
            new_candidate = new_child, child_fitness
            
            # Add child to new generation
            new_generation.append(new_candidate)
        
        return new_generation
        
    def ga_mutate(self, parents_to_keep, children):
        self.population = parents_to_keep + children
        
        # Rank the population according to their fitness in descending order
        self.population.sort(key=lambda t: t[1], reverse=True)
        
        # Mutate least performing N number of encodings by random choice
        mutated_count = 0
        for p_ix in range((self.population_size - self.mutation_size), self.population_size):
            # Get the target for mutation
            mutation_target:List[str]
            mutation_fitness:float
            mutation_target, mutation_fitness = self.population[p_ix]
            
            # Apply random mutation and update its fitness
            # Update the target with a random probability
            worker_list:List[str] = list(self.workers.keys())
            
            for m_ix, m_t in enumerate(mutation_target):
                prob_weights = [self.mutation_probability if w_l == m_t else (1.0 - self.mutation_probability)/(len(worker_list)-1) for w_l in worker_list]
                change = np.random.choice(a=worker_list, size=1, p=prob_weights)
                mutation_target[m_ix] = change[0]
            
            # Update the fitness
            mutation_fitness = self.calculate_fitness(mutation_target)
            
            # Update the citizen
            self.population[p_ix] = (mutation_target, mutation_fitness)
            
            mutated_count += 1
            
        if mutated_count != self.mutation_size:
            raise Exception(f"Number of mutations does not match what is required: {mutated_count}!")
            
    def initialize_population(self):
        generation = []
        fixed_pop_size = self.population_size
        
        # Append the last best found if exists
        if self.last_best_encoding:
            last_enc = self.last_best_encoding, self.calculate_fitness(self.last_best_encoding)
            generation.append(last_enc)
            fixed_pop_size -= 1
            
        for _ in range(fixed_pop_size):
            generation.append(self.generate_citizen())
        return generation

    def calculate_fitness(self, encoding:List[str]):
        executor_keys = list(self.workers.keys())
        worker_load: dict[str, int] = {}
        for key in executor_keys:
            worker_load[key] = 0
        visited_steps:List[str] = []

        maximum_path_cost:float = 0.0
        crr_enc_ix = 0

        worker_intake: dict[str, int] = {}
        for key in executor_keys:
            worker_intake[key] = 0

        for path in self.topology.get_all_simple_paths():
            current_path_cost:float = 0.0
            prev_output_target:str = ""
            prev_output_topic:str = ""
            step:TopologyNode
            for step in path:
                if step.node_type in (NodeType.SINK, NodeType.HEAD):
                    continue
                if step.node_type == NodeType.PRODUCER:
                    node_data:RawSettings = step.node_data
                    # Choose a random location to place write the output to
                    raw_storage_host:str = encoding[crr_enc_ix]
                    if self.last_best_encoding and crr_enc_ix in self.last_best_encoding:
                        prev_location = self.last_best_encoding[crr_enc_ix]
                    else:
                        prev_location = raw_storage_host
                    crr_enc_ix+=1
                    prev_output_target = raw_storage_host
                    prev_output_topic = node_data.output_topics[0].output_topic

                    if step.name not in visited_steps:
                        worker_intake[raw_storage_host] += 1
                        visited_steps.append(step.name)
                        if worker_intake[raw_storage_host] > self.device_limit:
                            return 0.0

                    # Add to current path cost
                    low_cost, high_cost = self.get_raw_setting_cost(node_data)
                    
                    if low_cost == 0 or high_cost == 0:
                        raise Exception("Cost cannot be zero at any point.")
                    
                    if node_data.producer_name != raw_storage_host:
                        if prev_location != raw_storage_host:
                            current_path_cost += (high_cost * self.migration_penalty)
                        else:
                            current_path_cost += high_cost
                    else:
                        if prev_location != raw_storage_host:
                            current_path_cost += (low_cost * self.migration_penalty)
                        else:
                            current_path_cost += low_cost

                if step.node_type == NodeType.EXECUTER:
                    # Choose a random location to execute the event at
                    execution_host:str = encoding[crr_enc_ix]
                    crr_enc_ix+=1
                    
                    # Choose a random location to place write the output to
                    output_storage_host:str = encoding[crr_enc_ix]
                    if self.last_best_encoding and crr_enc_ix in self.last_best_encoding:
                        prev_output_location = self.last_best_encoding[crr_enc_ix]
                    else:
                        prev_output_location = output_storage_host
                    crr_enc_ix+=1
                    
                    # Invalid if the number of tasks in a device is larger than the allowed
                    # device limit!
                    if step.name not in visited_steps:
                        worker_load[execution_host] += 1
                        worker_intake[output_storage_host] += 1
                        if worker_load[execution_host] > self.device_limit:
                            return 0.0
                        if worker_intake[output_storage_host] > self.device_limit:
                            return 0.0                        
                        visited_steps.append(step.name)

                    # Add to current path cost
                    if step.name not in self.task_execution_costs or prev_output_topic not in self.task_reading_costs[step.name]:
                        execution_cost = 5.0
                        reading_cost = 5.0
                        output_cost = 5.0
                    else:
                        execution_cost_keeper: TopicKeeper = self.task_execution_costs[step.name]
                        reading_cost_keeper: TopicKeeper = self.task_reading_costs[step.name][prev_output_topic]
                        output_cost_keeper: TopicKeeper = self.task_output_costs[step.name]
                        
                        if execution_cost_keeper.count == 0 or reading_cost_keeper.count == 0 or output_cost_keeper.count == 0:
                            execution_cost = 5.0
                            reading_cost = 5.0
                            output_cost = 5.0
                        else:
                            execution_cost = execution_cost_keeper.duration * 1.0 / execution_cost_keeper.size
                            reading_cost = reading_cost_keeper.duration * 1.0 / reading_cost_keeper.size
                            output_cost = output_cost_keeper.duration * 1.0 / output_cost_keeper.size
                                        
                    if execution_cost == 0 or reading_cost == 0 or output_cost == 0:
                        raise Exception("Cost cannot be zero at any point.")
                    
                    current_path_cost += execution_cost
                    if prev_output_target == execution_host:
                        current_path_cost += reading_cost
                    else:
                        current_path_cost += reading_cost * self.remote_cost_multiplier
                    
                    if execution_host == output_storage_host:
                        if prev_output_location != output_storage_host:
                            current_path_cost += (output_cost * self.migration_penalty)
                        else:
                            current_path_cost += output_cost
                    else:
                        if prev_output_location != output_storage_host:
                            current_path_cost += (output_cost * self.remote_cost_multiplier * self.migration_penalty)
                        else:
                            current_path_cost += output_cost * self.remote_cost_multiplier

                    cep_task:CEPTask = step.node_data
                    prev_output_topic = cep_task._settings.output_topics[0].output_topic                        

            maximum_path_cost = current_path_cost if current_path_cost > maximum_path_cost else maximum_path_cost

        if maximum_path_cost == 0:
            raise Exception("maximum_path_cost cannot be zero at any point.")

        # for _, w_val in worker_load.items():
        #     if w_val < configs.DEVICE_ACTION_MIN_LIMIT:
        #         return encoding, 0.0

        # for _, w_val in worker_intake.items():
        #     if w_val < configs.DEVICE_INTAKE_MIN_LIMIT:
        #         return encoding, 0.0

        return (1.0 / maximum_path_cost)

    # A population is basically a distribution of the targets
    def generate_citizen(self):        
        worker_keys = list(self.workers.keys())
        executor_keys = list(self.workers.keys())
        reader_keys = list(self.workers.keys())
        maximum_path_cost:float = 0.0
        worker_load: dict[str, int] = {}
        for key in executor_keys:
            worker_load[key] = 0

        worker_intake: dict[str, int] = {}
        for key in reader_keys:
            worker_intake[key] = 0

        processed_steps:dict[str, str] = {}
        processed_outputs:dict[str, str] = {}

        encoding = []

        for path in self.topology.get_all_simple_paths():
            current_path_cost:float = 0.0
            prev_output_target:str = ""
            prev_output_topic:str = ""
            step:TopologyNode
            for step in path:
                if step.node_type in (NodeType.SINK, NodeType.HEAD):
                    continue
                if step.node_type == NodeType.PRODUCER:
                    node_data:RawSettings = step.node_data
                    # Choose a random location to place write the output to
                    if step.name in processed_steps:
                        raw_storage_host:str = processed_steps[step.name]
                    else:
                        raw_storage_host:str = np.random.choice(reader_keys)
                        processed_steps[step.name] = raw_storage_host
                        worker_intake[raw_storage_host] += 1
                        if worker_intake[raw_storage_host] >= self.device_limit:
                            reader_keys.remove(raw_storage_host)
                        
                    prev_output_target = raw_storage_host
                    prev_output_topic = node_data.output_topics[0].output_topic
                    encoding.append(raw_storage_host)
                    
                    # Add to current path cost
                    low_cost, high_cost = self.get_raw_setting_cost(node_data)
                    
                    if low_cost == 0 or high_cost == 0:
                        raise Exception("Cost cannot be zero at any point.")
                    
                    if node_data.producer_name != raw_storage_host:
                        current_path_cost += high_cost
                    else:
                        current_path_cost += low_cost

                if step.node_type == NodeType.EXECUTER:
                    # Choose a random location to execute the event at
                    if step.name in processed_steps:
                        execution_host:str = processed_steps[step.name]
                    else:
                        execution_host:str = np.random.choice(executor_keys)
                        processed_steps[step.name] = execution_host
                        worker_load[execution_host] += 1
                        if worker_load[execution_host] >= self.device_limit:
                            executor_keys.remove(execution_host)

                    encoding.append(execution_host)
                    
                    # Choose a random location to place write the output to
                    if step.name in processed_outputs:
                        output_storage_host:str = processed_outputs[step.name]
                    else:
                        output_storage_host:str = np.random.choice(reader_keys)
                        processed_outputs[step.name] = output_storage_host
                        worker_intake[output_storage_host] += 1
                        if worker_intake[output_storage_host] >= self.device_limit:
                            reader_keys.remove(output_storage_host)

                    encoding.append(output_storage_host)

                    # Add to current path cost
                    if step.name not in self.task_execution_costs or prev_output_topic not in self.task_reading_costs[step.name]:
                        execution_cost = 5.0
                        reading_cost = 5.0
                        output_cost = 5.0
                    else:
                        execution_cost_keeper: TopicKeeper = self.task_execution_costs[step.name]
                        reading_cost_keeper: TopicKeeper = self.task_reading_costs[step.name][prev_output_topic]
                        output_cost_keeper: TopicKeeper = self.task_output_costs[step.name]
                        
                        if execution_cost_keeper.count == 0 or reading_cost_keeper.count == 0 or output_cost_keeper.count == 0:
                            execution_cost = 5.0
                            reading_cost = 5.0
                            output_cost = 5.0
                        else:
                            execution_cost = execution_cost_keeper.duration * 1.0 / execution_cost_keeper.size
                            reading_cost = reading_cost_keeper.duration * 1.0 / reading_cost_keeper.size
                            output_cost = output_cost_keeper.duration * 1.0 / output_cost_keeper.size
                                        
                    if execution_cost == 0 or reading_cost == 0 or output_cost == 0:
                        raise Exception("Cost cannot be zero at any point.")
                    
                    current_path_cost += execution_cost
                    if prev_output_target == execution_host:
                        current_path_cost += reading_cost
                    else:
                        current_path_cost += reading_cost * self.remote_cost_multiplier
                    
                    if execution_host == output_storage_host:
                        current_path_cost += output_cost
                    else:
                        current_path_cost += output_cost * self.remote_cost_multiplier
                        
                    cep_task:CEPTask = step.node_data
                    prev_output_topic = cep_task._settings.output_topics[0].output_topic

            maximum_path_cost = current_path_cost if current_path_cost > maximum_path_cost else maximum_path_cost

        if maximum_path_cost == 0:
            raise Exception("maximum_path_cost cannot be zero at any point.")
        
        # for _, w_val in worker_load.items():
        #     if w_val < configs.DEVICE_ACTION_MIN_LIMIT:
        #         return encoding, 0.0
            
        # for _, w_val in worker_intake.items():
        #     if w_val < configs.DEVICE_INTAKE_MIN_LIMIT:
        #         return encoding, 0.0

        return encoding, (1.0 / maximum_path_cost)

    def get_raw_setting_cost(self, n_data: RawSettings) -> tuple[float, float]:
        # common variables
        raw_name: str = n_data.raw_data_name
        
        if raw_name in self.raw_output_costs.keys():
            return self.raw_output_costs[raw_name]
        
        n_stats: RawStatisticsBook = self.producer_records[raw_name]

        # cost calculation
        producer_low_cost = 0
        producer_high_cost = 0
        if not n_stats or n_stats.write_size_byte_total == 0:
            producer_low_cost = n_data.expected_cost
            producer_high_cost = math.ceil(n_data.expected_cost * self.remote_cost_multiplier)
        else:
            producer_write_cost = (
                n_stats.write_time_ns_total * 1.0
                / n_stats.write_size_byte_total
            )
            producer_low_cost = producer_write_cost
            producer_high_cost = (
                math.ceil(producer_write_cost * self.remote_cost_multiplier)
            )
            
        self.raw_output_costs[raw_name] = producer_low_cost, producer_high_cost
        return self.raw_output_costs[raw_name]

    def event_statistics(self):
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
                        nw_cpu_tk.count = stat_record.crr_cpu_usage_stats.count
                        nw_cpu_tk.size = stat_record.crr_cpu_usage_stats.total                            
                        cpu_usages[stat_record.action_name] = nw_cpu_tk
                    else:
                        crr_cpu_tk: TopicKeeper = cpu_usages[stat_record.action_name]
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
                        crr_tk.count += stat_record.crr_data_write_ns_stats.count
                        crr_tk.duration += stat_record.crr_data_write_ns_stats.total
                        crr_tk.size += stat_record.crr_out_data_byte_stats.total
                    else:
                        new_tk = TopicKeeper()
                        new_tk.count += stat_record.crr_data_write_ns_stats.count
                        new_tk.duration += stat_record.crr_data_write_ns_stats.total
                        new_tk.size += stat_record.crr_out_data_byte_stats.total
                        task_output_costs[stat_record.action_name] = new_tk

                    # Action-wise total duration spent
                    if stat_record.action_name in task_execution_costs:
                        crr_tk: TopicKeeper = task_execution_costs[
                            stat_record.action_name
                        ]
                        crr_tk.count += stat_record.crr_task_execution_ns_stats.count
                        crr_tk.duration += stat_record.crr_task_execution_ns_stats.total
                        crr_tk.size += stat_record.crr_in_data_byte_stats.total
                    else:
                        new_tk = TopicKeeper()
                        new_tk.count += stat_record.crr_task_execution_ns_stats.count
                        new_tk.duration += stat_record.crr_task_execution_ns_stats.total
                        new_tk.size += stat_record.crr_in_data_byte_stats.total
                        task_execution_costs[stat_record.action_name] = new_tk

        return task_execution_costs, topic_data_stats, task_output_costs, cpu_usages

    def get_final_distribution(self, encoding):
        # Encoding is of format step-targets, cost
        step_targets, total_cost = encoding
        
        # Loop through the paths and arrange the final asignments
        print("Arranging the GA assignments...")

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []

        # deepcopy of workers in case of manipulation
        workers = deepcopy(self.workers)

        n_tasks_activated = 0
        n_tasks_deactivated = 0

        step_ix = 0
        processed_steps:List[str] = []
        for path in self.topology.get_all_simple_paths():
            step:TopologyNode
            for step in path:
                if step.node_type in (NodeType.HEAD, NodeType.SINK):
                    continue
                if step.node_type == NodeType.PRODUCER:
                    current_raw_target = step_targets[step_ix]
                    step_ix += 1

                    if step.name in processed_steps:
                        continue

                    producer: RawSettings = step.node_data
                    producer_target_db_update = RawUpdateEvent()
                    producer_target_db_update.producer_name = producer.producer_name
                    producer_target_db_update.output_topics = producer.output_topics
                    producer_updates.append(producer_target_db_update)
                    
                    topic_list = []
                    for output_topic in producer.output_topics:
                        output_topic.target_database = current_raw_target
                        topic_list.append(output_topic.output_topic)
                        print(f"Raw source {step.name} will write its output to: {current_raw_target}")
                        
                    # Update where successors read from for this topic
                    successors = self.topology.get_successors(step)
                    
                    if len(successors) == 0:
                        raise Exception("All raw data sources are required to have successors as events!")
                    
                    succ:TopologyNode
                    for succ in successors:
                        if succ.node_type in (NodeType.HEAD, NodeType.SINK):
                            continue 
                        succ_data:CEPTask = succ.node_data
                        for req in succ_data._settings.required_sub_tasks:
                            if req.input_topic in topic_list:
                                req.stored_database = current_raw_target
                                print(f"Event {succ.name} for topic {req.input_topic} will read its input from: {current_raw_target}")
                                
                    processed_steps.append(step.name)

                if step.node_type == NodeType.EXECUTER:
                    current_execution_target = step_targets[step_ix]
                    step_ix += 1
                    current_output_target = step_targets[step_ix]
                    step_ix += 1

                    if step.name in processed_steps:
                        continue


                    print(f"Event {step.name} will be executed at : {current_execution_target}")

                    # Task info
                    task: CEPTask = step.node_data

                    # assigning where the outputs will be written
                    output_topics = []
                    for rot in task._settings.output_topics:
                        rot.target_database = current_output_target
                        output_topics.append(rot.output_topic)
                        
                    # Update successors input sources
                    successors = self.topology.get_successors(step)
                    succ:TopologyNode
                    for succ in successors:
                        if succ.node_type in (NodeType.HEAD, NodeType.SINK):
                            continue                         
                        succ_data:CEPTask = succ.node_data
                        for req in succ_data._settings.required_sub_tasks:
                            if req.input_topic in output_topics:
                                req.stored_database = current_output_target
                                print(f"Event {succ.name} for topic {req.input_topic} will read its input from: {current_output_target}")

                    # where the code will be executed
                    task._settings.host_name = current_execution_target

                    alteration = TaskAlterationModel()
                    alteration.host = current_execution_target
                    alteration.activate = True
                    alteration.job_name = task._settings.action_name
                    alteration.cep_task = task
                    alterations.append(alteration)
                    n_tasks_activated += 1

                    # for non chosen hosts this task should be deactivated
                    for host in [*workers]:
                        if host != current_execution_target:
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
                            
                    processed_steps.append(step.name)

        print(
            "Number of tasks activated is: ",
            n_tasks_activated,
            " deactivated: ",
            n_tasks_deactivated,
        )

        print(f"Distribution finished with final cost: {total_cost}")
        return alterations, producer_updates, step_targets

def genetic_algorithm(
    workers, topology: Topology, statistic_records, producer_records, last_best_encoding
) -> tuple[List[TaskAlterationModel], List[RawUpdateEvent]]:
    print("Running Genetic algorithm distribution...")
    ga = CEPGeneticAlgorithm(
        workers=workers,
        topology=topology,
        statistic_records=statistic_records,
        producer_records=producer_records,
        last_best_encoding=last_best_encoding
        )
    
    return ga.solve()

