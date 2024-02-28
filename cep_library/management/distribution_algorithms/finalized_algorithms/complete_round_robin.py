from copy import deepcopy
from typing import List

from cep_library.cep.model.cep_task import CEPTask, RequiredInputTopics
from cep_library.management.model.task_alteration import (
    DataMigrationModel,
    TaskAlterationModel,
)
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent


def complete_round_robin(
    workers,
    topology: Topology,
    statistic_records,
    producer_records,
    distribution_history,
) -> tuple[List[TaskAlterationModel], List[RawUpdateEvent], str]:
    try:
        print(
            "Running algorithm number 21: Complete round robin without considering the data sources..."
        )

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []

        # If no workers are registered yet no need to go further
        if not workers:
            return alterations, producer_updates, None

        # deepcopy of workers in case of manipulation
        workers = deepcopy(workers)
        worker_keys = list(workers.keys())

        # sort the worker according to keys
        worker_keys.sort()

        print("Sorted worker list: ", worker_keys)

        # Distribute the tasks with a very basic rule
        # Initial tasks that require raw data goes to the devices where that data is produced
        # Rest of the tasks are distributed in a round-robin approach to all of the devices.
        # Generate the task alteration requests randomly
        n_tasks_activated = 0
        n_tasks_deactivated = 0

        first_target_host_name = distribution_history
        task_nodes = topology.get_executor_nodes()
        task_nodes.sort(key=lambda x: x.name)
        prod_nodes = topology.get_producer_nodes()
        prod_nodes.sort(key=lambda x: x.name)

        # Get the index of the last first target host name
        # Which corresponds to where the first task in the sorted order is placed at!
        if first_target_host_name:
            first_target_ix = worker_keys.index(first_target_host_name)

            # Calculate next target index
            # It was the last index on last run
            if first_target_ix == len(worker_keys) - 1:
                next_target_ix = 0
            # It was not the last
            else:
                next_target_ix = first_target_ix + 1
        # No previous dist data exists
        else:
            next_target_ix = 0

        # Update the first targeted action name data
        first_target_host_name = worker_keys[next_target_ix]

        # Iterate through the producers first
        prod_node: TopologyNode
        for prod_node in prod_nodes:
            # Predecessor data and where it writes its output
            producer: RawSettings = prod_node.node_data
            producer_target_db_update = RawUpdateEvent()
            producer_target_db_update.producer_name = producer.producer_name
            producer_target_db_update.output_topics = producer.output_topics
            producer_target_db_update.raw_name = producer.raw_data_name
            producer_updates.append(producer_target_db_update)
            for output_topic in producer.output_topics:
                print(
                    "[BFS] Currently processing producer node: ",
                    producer.producer_name,
                    " ",
                    output_topic.output_topic,
                )
                output_topic.target_database = producer.producer_name

            print(f"Assigning production: {producer.raw_data_name}, to: {producer.producer_name}")

        # Iterate through tasks one time to assign their hosts
        crr_target_ix = next_target_ix
        task_node: TopologyNode
        for task_node in task_nodes:
            # Task info
            task: CEPTask = task_node.node_data

            # Execution host
            # TODO: think about the case when the number of tasks is large
            # than the number of devices, how will round robin work then?
            current_execution_host = worker_keys[crr_target_ix]

            print(f"Assigning task: {task._settings.action_name}, to: {current_execution_host}")

            # Update current worker pointer
            if crr_target_ix == len(worker_keys) - 1:
                crr_target_ix = 0
            else:
                crr_target_ix += 1

            # Assign where the task will be executed at and where the output will be written
            task._settings.host_name = current_execution_host
            for output_topic in task._settings.output_topics:
                output_topic.target_database = current_execution_host

        # Iterate through the task nodes one last time to assign where input
        # topics will read their data from according to assignments in previous step
        # Also prepare the task/event alteration requests
        task_node: TopologyNode
        for task_node in task_nodes:
            # Task info
            task: CEPTask = task_node.node_data

            predessors = topology.get_predecessors(task_node)

            pred: TopologyNode
            for pred in predessors:
                if pred.node_type == NodeType.PRODUCER:
                    # Required topics related to this node
                    rst: RequiredInputTopics
                    for rst in task._settings.required_sub_tasks:
                        prod_topic = [
                            t
                            for t in pred.node_data.output_topics
                            if t.output_topic == rst.input_topic
                        ]
                        if prod_topic:
                            rst.stored_database = prod_topic[0].target_database

                if pred.node_type == NodeType.EXECUTER:
                    pred_data: CEPTask = pred.node_data

                    # required topic related to this node
                    rst: RequiredInputTopics
                    for rst in task._settings.required_sub_tasks:
                        pred_topics = [
                            t
                            for t in pred_data._settings.output_topics
                            if t.output_topic == rst.input_topic
                        ]
                        if pred_topics:
                            rst.stored_database = pred_topics[0].target_database

            # Since BFS hierarchy might hit some nodes before it is intended we also need to update
            # the successors if needed.
            successors = topology.get_successors(task_node)

            suc: TopologyNode
            for suc in successors:
                if (
                    suc.node_type != NodeType.EXECUTER
                    and suc.node_type != NodeType.SINK
                ):
                    raise Exception("Successor node type is invalid!")

                if suc.node_type == NodeType.SINK:
                    continue

                # Update the required input topic sources of the successor
                # a.k.a. where they will read the data
                suc_data: CEPTask = suc.node_data
                for rst in suc_data._settings.required_sub_tasks:
                    matched_source = [
                        t
                        for t in task._settings.output_topics
                        if t.output_topic == rst.input_topic
                    ]
                    if matched_source:
                        rst.stored_database = matched_source[0].target_database

            # region alterations
            alteration = TaskAlterationModel()
            alteration.host = task._settings.host_name
            alteration.activate = True
            alteration.job_name = task._settings.action_name
            alteration.cep_task = task
            alterations.append(alteration)
            n_tasks_activated += 1

            # for non chosen hosts this task should be deactivated
            for host in [*workers]:
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

            # endregion alterations

        # visualize it again to see the labels for code and data location assignments
        # carrying data after changing its saving location!
        # topology.visualize_network()
        topology.print_graph_test()
        print(
            "Number of tasks activated is: ",
            n_tasks_activated,
            " deactivated: ",
            n_tasks_deactivated,
            " first assigned host was: ",
            first_target_host_name,
        )

        return alterations, producer_updates, first_target_host_name
    except Exception as e:
        print("[ERROR during algorithmic distribution]: ", str(e))
        return [], [], None
