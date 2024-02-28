from copy import deepcopy
from typing import List

from cep_library import configs
from cep_library.cep.model.cep_task import CEPTask, RequiredInputTopics
from cep_library.management.distribution_algorithms.helpers.OutputForwarder import OutputForwarder
from cep_library.management.distribution_algorithms.helpers.StatChecker import StatChecker
from cep_library.management.model.task_alteration import DataMigrationModel, TaskAlterationModel
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent


# Sets the composite actions host as the next available host among the data providers.
def greedy(workers, topology:Topology, statistic_records, producer_records) -> tuple[List[TaskAlterationModel], List[RawUpdateEvent]]:
    # try:
        # Helpers
        output_forwarder = OutputForwarder()
        stat_checker = StatChecker()
                
        print("Running algorithm number 8...")

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []
                            
        # If no workers are registered yet no need to go further
        if not workers: return alterations, producer_updates
        
        # deepcopy of workers and statistics in case of manipulation 
        node_records = deepcopy(statistic_records)
        workers = deepcopy(workers)      
        worker_keys = list(workers.keys())
               
        # sort the worker according to keys
        worker_keys.sort()
        
        worker_fill = {}
        for worker_key in worker_keys:
            if configs.DATA_LOCALITY_WORKER_ACTION_LIMIT == 0:
                worker_fill[worker_key] = 99
            elif configs.DATA_LOCALITY_WORKER_ACTION_LIMIT > 0:
                worker_fill[worker_key] = configs.DATA_LOCALITY_WORKER_ACTION_LIMIT        
        
        print("Sorted worker list: ", worker_keys)
                    
        output_costs = stat_checker.get_current_output_cost_of_actions(node_records)
        input_costs = stat_checker.get_current_input_cost_of_actions(node_records)
                    
        # Distribute the tasks with a very basic rule
        # Initial tasks that require raw data goes to the devices where that data is produced
        # Rest of the tasks are distributed in a round-robin approach to all of the devices.        
        # Generate the task alteration requests randomly
        n_tasks_activated = 0
        n_tasks_deactivated = 0
        
        # BFS from the root node and assign where data is stored as well as where task will be executed
        print("Traversing the tree with BFS algorithm...")
        bfs_tree = topology.get_bfs()
        node:TopologyNode
        crr_bfs_node_number = 0
        for node, _ in bfs_tree.nodes(data=True):
            if node.processed == True: continue
            
            if node.node_type == NodeType.PRODUCER or node.node_type == NodeType.EXECUTER:
                print("Node number: ", crr_bfs_node_number)
                crr_bfs_node_number+=1
            
            # Producers are first encountered typed nodes in the tree
            # since source is connected to them directly
            # In order to change the target database manipulate the target_database 
            # variable in the output_topics
            if node.node_type == NodeType.PRODUCER:
                producer:RawSettings = node.node_data
                producer_target_db_update = RawUpdateEvent()
                producer_target_db_update.producer_name = producer.producer_name
                producer_target_db_update.output_topics = producer.output_topics
                producer_updates.append(producer_target_db_update)                
                node.processed = True
                for output_topic in producer.output_topics:
                    print("[BFS] Currently processing producer node: ", producer.producer_name, " " ,  output_topic.output_topic)
                
            # Rest are executors and the final sink which is redundant
            # Since immediate executors are processed previously we need to update
            # the preferred databases of the previous nodes if round-robin requires us to do so
            if node.node_type == NodeType.EXECUTER:
                # Task info
                task:CEPTask=node.node_data
                
                # If there is only a single input_topic that the method listens to
                # then assign that code execution to where that data is produced
                trigger_count = len(task._settings.required_sub_tasks)
                                    
                # and where the output of that code will be written at
                # get the list of hosts where the predecessor of this node writes their data to
                # find the required sub topic that the predecessor will feed and set its stored host value
                # so that it can be read from there
                predessors = topology.get_predecessors(node)
                if not predessors:
                    print("All executors should have a predecessor!")
                    return [], []
                
                # list of available hosts                    
                available_source_hosts = {}
                                                
                pred:TopologyNode
                for pred in predessors:
                    if pred.node_type == NodeType.PRODUCER:                            
                        # predecessor data and where it writes its output
                        prod_data:RawSettings = pred.node_data
                        
                        # If there is only one required sub task than it should have 
                        # one predecessor for now
                        if trigger_count == 1:
                            execution_host = prod_data.producer_name
                        else:
                            raise Exception("If a predecessor is producer then only one input can be active!")
                        
                        # required topic related to this node
                        rst:RequiredInputTopics
                        for rst in task._settings.required_sub_tasks:
                            prod_topic = [t for t in prod_data.output_topics if t.output_topic == rst.input_topic]
                            if prod_topic:
                                print("checking a producer predecessor: ", prod_topic)
                                rst.stored_database = prod_topic[0].target_database
                                                            
                    if pred.node_type == NodeType.EXECUTER:
                        pred_data:CEPTask = pred.node_data

                        # If there is only one required sub task than it should have 
                        # one predecessor for now
                        if trigger_count == 1:
                            execution_host = pred_data._settings.host_name
                        else:
                            if pred_data._settings.host_name not in available_source_hosts and pred_data._settings.host_name:
                                if pred_data._settings.action_name in output_costs:
                                    available_source_hosts[pred_data._settings.host_name] = output_costs[pred_data._settings.action_name]
                                else:
                                    available_source_hosts[pred_data._settings.host_name] = 0.0
                                    
                            elif pred_data._settings.host_name in available_source_hosts:
                                if pred_data._settings.action_name in output_costs:
                                    if output_costs[pred_data._settings.action_name] > available_source_hosts[pred_data._settings.host_name]:
                                        available_source_hosts[pred_data._settings.host_name] = output_costs[pred_data._settings.action_name]

                        # required topic related to this node
                        # to determine where to read the input data from for each required source
                        rst:RequiredInputTopics
                        for rst in task._settings.required_sub_tasks:
                            pred_topics = [t for t in pred_data._settings.output_topics if t.output_topic == rst.input_topic]
                            if pred_topics:
                                print("checking a executer predecessor: ", pred_topics[0].output_topic)
                                if hasattr(pred_topics[0], 'target_database'):
                                    rst.stored_database = pred_topics[0].target_database

                # if trigger coutn is larger than one
                # choose from one of the hosts in a round-robin approach
                # WITHOUT checking the source
                if trigger_count > 1:
                    # Sort the keys according to the output byte values
                    sorted_hosts = [k for k, _ in sorted(available_source_hosts.items(), key=lambda item: item[1], reverse=True)]
                    execution_host = sorted_hosts[0]

                # If the host hit its limit put the execution to another device
                if worker_fill[execution_host] == 0:
                    # finding another device...
                    for key in worker_fill:
                        if worker_fill[key] > 0:
                            execution_host = key
                            worker_fill[key] = worker_fill[key] - 1
                            break
                else:
                    worker_fill[execution_host] = worker_fill[execution_host] - 1

                # If a device is already assigned find the next device in the available sources list
                print("[BFS] Current host for this composite task was: ", task._settings.host_name)
                print("[BFS] Current available hosts for this composite task are: ", available_source_hosts)
                if trigger_count > 1:
                    print("[BFS] Current sorted hosts for this composite task are: ", sorted_hosts)

                # assigning where the outputs will be written
                # here they are written where they are produced
                for rot in task._settings.output_topics:
                    print("[BFS] Currently processing executor node: ", execution_host, " " ,  rot.output_topic)
                    rot.target_database = execution_host                                                     
                
                # Update where this codes predecessor will write their data
                output_forwarder.forward_predecessor_outputs(input_costs, node, predessors, execution_host, topology)
                
                # Since BFS hierarchy might hit some nodes before it is intended we also need to update
                # the successors if needed.
                successors = topology.get_successors(node)
                if not successors:
                    print("All executors should have a successor!")
                    return [], []
                
                suc:TopologyNode
                for suc in successors:
                    if suc.node_type != NodeType.EXECUTER and suc.node_type != NodeType.SINK:
                        raise Exception("Successor node type is invalid!")
                    
                    if suc.node_type == NodeType.SINK:
                        continue
                    
                    # Update the required input topic sources of the successor
                    # a.k.a. where they will read the data 
                    suc_data:CEPTask = suc.node_data
                    for rst in suc_data._settings.required_sub_tasks:
                        matched_source = [t for t in task._settings.output_topics if t.output_topic == rst.input_topic]
                        if matched_source:
                            rst.stored_database = matched_source[0].target_database
                
                # where the code will be executed
                task._settings.host_name = execution_host
                                        
                #region alterations
                alteration = TaskAlterationModel()
                alteration.host = execution_host
                alteration.activate = True
                alteration.job_name = task._settings.action_name
                alteration.cep_task = task
                alterations.append(alteration)
                n_tasks_activated+=1
                
                # for non chosen hosts this task should be deactivated
                for host in [*workers]:
                    if host != execution_host:
                        # assumes that only one output topic is written/being published to
                        migration_request = DataMigrationModel()
                        migration_request.topic = task._settings.output_topics[0].output_topic
                        migration_request.current_host = host
                        migration_request.target_host = task._settings.host_name                        
                        
                        deactivation = TaskAlterationModel()
                        deactivation.host = host
                        deactivation.activate = False
                        deactivation.job_name = task._settings.action_name
                        deactivation.migration_requests = [migration_request]
                        alterations.append(deactivation)
                        n_tasks_deactivated+=1         
                            
                node.processed = True
                #endregion alterations
            
        # visualize it again to see the labels for code and data location assignments
        # carrying data after changing its saving location!
        # topology.visualize_network()
        topology.print_graph_test()
        print("Number of tasks activated is: ", n_tasks_activated, " deactivated: ", n_tasks_deactivated)
        
        return alterations, producer_updates
    # except Exception as e:
    #     print("[ERROR during algorithmic distribution]: ", str(e))
    #     return [], []
