    
from cep_library.cep.model.cep_task import CEPTask
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.raw.model.raw_settings import RawSettings


class OutputForwarder:
    def __init__(self) -> None:
        pass
    
    def forward_predecessor_outputs(self, input_costs:dict, requester:TopologyNode, predessors:list, output_target:str, topology:Topology) -> None:            
        pred:TopologyNode
        for pred in predessors:
            if pred.node_type == NodeType.HEAD or pred.node_type == NodeType.SINK:
                continue
            
            # Update only if the requester successor is the succesor task with currently highest traffic
            # Successors are bound to be either single or composite tasks
            successors = topology.get_successors(pred)
            success_costs = {}
            successor:TopologyNode
            for successor in successors:
                succ_data:CEPTask = successor.node_data
                if succ_data._settings.action_name in input_costs:
                    success_costs[successor] = input_costs[succ_data._settings.action_name]
                else:
                    success_costs[successor] = 0.0
                
            # Sort by cost and check if highest one is the requestor, if not skip
            sorted_successors = [k for k, _ in sorted(success_costs.items(), key=lambda item: item[1], reverse=True)]
            if sorted_successors[0] != requester:
                continue
            
            if pred.node_type == NodeType.PRODUCER:
                prod_data:RawSettings = pred.node_data

                # Update where the data is written
                for rot in prod_data.output_topics:
                    print("[BFS] Currently forwarding producer node output: ", pred.name, " " ,  rot.output_topic)
                    rot.target_database = output_target
                    
                self.update_producer_successor(pred, prod_data, topology)
                
            if pred.node_type == NodeType.EXECUTER:
                exec_data:CEPTask = pred.node_data
                
                # Update where the data is written
                for rot in exec_data._settings.output_topics:
                    print("[BFS] Currently forwarding producer node output: ", pred.name, " " ,  rot.output_topic)
                    rot.target_database = output_target
                    
                self.update_executor_successor(pred, exec_data, topology)
                                    
    def update_producer_successor(self, pred:TopologyNode, exec_data:RawSettings, topology:Topology):
        # Since BFS hierarchy might hit some nodes before it is intended we also need to update
        # the successors if needed.
        successors = topology.get_successors(pred)
        if not successors:
            print("All executors should have a successor!")
            return [], []
        
        suc:TopologyNode
        for suc in successors:
            if suc.node_type != NodeType.EXECUTER:
                continue
            
            # Update the required input topic sources of the successor
            # a.k.a. where they will read the data 
            suc_data:CEPTask = suc.node_data
            for rst in suc_data._settings.required_sub_tasks:
                matched_source = [t for t in exec_data.output_topics if t.output_topic == rst.input_topic]
                if matched_source:
                    rst.stored_database = matched_source[0].target_database    
                                    
    def update_executor_successor(self, pred:TopologyNode, exec_data:CEPTask, topology:Topology):
        # Since BFS hierarchy might hit some nodes before it is intended we also need to update
        # the successors if needed.
        successors = topology.get_successors(pred)
        if not successors:
            print("All executors should have a successor!")
            return [], []
        
        suc:TopologyNode
        for suc in successors:
            if suc.node_type != NodeType.EXECUTER:
                continue
            
            # Update the required input topic sources of the successor
            # a.k.a. where they will read the data 
            suc_data:CEPTask = suc.node_data
            for rst in suc_data._settings.required_sub_tasks:
                matched_source = [t for t in exec_data._settings.output_topics if t.output_topic == rst.input_topic]
                if matched_source:
                    rst.stored_database = matched_source[0].target_database
            