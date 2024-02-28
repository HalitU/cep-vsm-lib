from copy import deepcopy
from typing import List

import numpy as np

from cep_library.cep.model.cep_task import CEPTask, RequiredInputTopics
from cep_library.management.model.task_alteration import (
    DataMigrationModel,
    TaskAlterationModel,
)
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent


def random_distribution(
    workers, topology: Topology, statistic_records, producer_records
) -> tuple[List[TaskAlterationModel], List[RawUpdateEvent]]:
    try:
        print("Running RANDOM distribution...")

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []

        # If no workers are registered yet no need to go further
        if not workers:
            return alterations, producer_updates

        # deepcopy of workers in case of manipulation
        workers = deepcopy(workers)
        worker_keys = list(workers.keys())

        # sort the worker according to keys
        worker_keys.sort()

        print("Sorted worker list: ", worker_keys)

        n_tasks_activated = 0
        n_tasks_deactivated = 0

        topology_nodes = topology.get_topological_ordered_nodes()
        node: TopologyNode
        crr_bfs_node_number = 0
        processed_worker_keys = []
        for node in topology_nodes:
            if (
                node.node_type == NodeType.PRODUCER
                or node.node_type == NodeType.EXECUTER
            ):
                print("Node number: ", crr_bfs_node_number)
                crr_bfs_node_number += 1

            # Producers write to where they are located at
            if node.node_type == NodeType.PRODUCER:
                producer: RawSettings = node.node_data
                producer_target_db_update = RawUpdateEvent()
                producer_target_db_update.producer_name = producer.producer_name
                producer_target_db_update.output_topics = producer.output_topics
                producer_updates.append(producer_target_db_update)
                
                for output_topic in producer.output_topics:
                    print(
                        "[BFS] Currently processing producer node: ",
                        producer.producer_name,
                        " ",
                        output_topic.output_topic,
                    )

            if node.node_type == NodeType.EXECUTER:
                # Task info
                task: CEPTask = node.node_data

                predessors = topology.get_predecessors(node)
                if not predessors:
                    print("All executors should have a predecessor!")
                    return [], []

                # Randomly get one of the workers
                execution_host = np.random.choice(worker_keys)
    
                print(f"Choose random host as {execution_host}")
                    
                # Logging the processed workers
                processed_worker_keys.append(execution_host)
                worker_keys.remove(execution_host)
                if len(worker_keys) == 0:
                    worker_keys = list(workers.keys())
                    processed_worker_keys.clear()

                # Update where data should be read from according to predecessors
                pred: TopologyNode
                for pred in predessors:
                    if pred.node_type == NodeType.PRODUCER:
                        # predecessor data and where it writes its output
                        prod_data: RawSettings = pred.node_data
                        rst: RequiredInputTopics
                        for rst in task._settings.required_sub_tasks:
                            prod_topic = [
                                t
                                for t in prod_data.output_topics
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

                # assigning where the outputs will be written
                # here they are written where they are produced
                for rot in task._settings.output_topics:
                    print(
                        "[BFS] Currently processing executor node: ",
                        execution_host,
                        " ",
                        rot.output_topic,
                    )
                    rot.target_database = execution_host

                # Since BFS hierarchy might hit some nodes before it is intended we also need to update
                # the successors if needed.
                successors = topology.get_successors(node)
                if not successors:
                    print("All executors should have a successor!")
                    return [], []

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

                # where the code will be executed
                task._settings.host_name = execution_host

                # region alterations
                alteration = TaskAlterationModel()
                alteration.host = execution_host
                alteration.activate = True
                alteration.job_name = task._settings.action_name
                alteration.cep_task = task
                alterations.append(alteration)
                n_tasks_activated += 1

                # for non chosen hosts this task should be deactivated
                for host in [*workers]:
                    if host != execution_host:
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
        )

        return alterations, producer_updates
    except Exception as e:
        print("[ERROR during algorithmic distribution]: ", str(e))
        return [], []
