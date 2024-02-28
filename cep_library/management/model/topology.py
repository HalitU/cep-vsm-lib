from enum import Enum
from pathlib import Path

import networkx as nx

from cep_library.cep.model.cep_task import CEPTask
from cep_library.raw.model.raw_settings import RawSettings


class NodeType(Enum):
    HEAD=0
    PRODUCER=1
    EXECUTER=2
    SINK=3

class TopologyNode:
    def __init__(self, node_type:NodeType, node_data, name) -> None:
        self.node_type = node_type
        self.node_data = node_data
        self.name = name
        self.processed = False

    def get_inputs_topics(self):
        match self.node_type:
            case NodeType.PRODUCER:
                raise Exception("Invalid node type for getting input topics!")
            case NodeType.EXECUTER:
                nd:CEPTask = self.node_data
                return [st.input_topic for st in nd._settings.required_sub_tasks]
        raise Exception("Invalid node type for getting input topics!")
    
    def get_output_topic(self):
        match self.node_type:
            case NodeType.PRODUCER:
                nd:RawSettings = self.node_data
                return nd.output_topics[0].output_topic
            case NodeType.EXECUTER:
                nd:CEPTask = self.node_data
                return nd._settings.output_topics[0].output_topic
        raise Exception("Invalid node to call output topics!")

    def to_text(self):
        try:
            if self.node_type == NodeType.SINK:
                return "sink"
            if self.node_type == NodeType.HEAD:
                return "source"
            if self.node_type == NodeType.PRODUCER:
                prod_data:RawSettings = self.node_data
                res = self.name + "\n"
                res += "producer outputs and target database\n"
                for row in [a.output_topic + " " + a.target_database + "\n" for a in prod_data.output_topics]:
                    res += row
                return res
            if self.node_type == NodeType.EXECUTER:
                exec_data:CEPTask = self.node_data
                print("Writing action: ", exec_data._settings.action_name)
                res = self.name
                res += "executor host: " + exec_data._settings.host_name + "\n"
                res += "executor input topics and source database\n"
                for row in [a.input_topic + " " + a.stored_database + "\n" for a in exec_data._settings.required_sub_tasks]:
                    res += row
                res += "executor output topics and target database\n"
                for row in [a.output_topic + " " + a.target_database + "\n" for a in exec_data._settings.output_topics]:
                    res += row
                return res
        except:
            return "undefined \n"

class Topology:
    def __init__(self) -> None:
        self.G = nx.DiGraph()
        self.Head = None
        self.graph_file = "visuals/graph_output.txt"
        self.current_host_code_dist: dict = None
    
        Path("visuals/").mkdir(parents=True, exist_ok=True)
        with open(self.graph_file, "w") as outfile:
            outfile.write("\n")
        outfile.close()    
    
    def add_host_code_relationship(self, host, action_name) -> None:
        if host in self.current_host_code_dist:
            self.current_host_code_dist[host].append(action_name)
        else:
            self.current_host_code_dist[host] = [action_name]
    
    def get_node_count(self):
        ctr = 0
        n:TopologyNode
        for n in self.G:
            if n.node_type == NodeType.EXECUTER or n.node_type == NodeType.PRODUCER:
                ctr += 1
        return ctr
    
    def get_node_counts(self):
        prod_count = 0
        exec_count = 0
        n:TopologyNode
        for n in self.G:
            if n.node_type == NodeType.EXECUTER:
                exec_count += 1
            if n.node_type == NodeType.PRODUCER:
                prod_count += 1
        return prod_count, exec_count
    
    # Another way to get root or sink etc:
    # [n for n,d in G.in_degree() if d==0]
    def get_all_simple_paths(self):
        source = None
        sink = None
        n:TopologyNode
        for n in self.G:
            if n.node_type == NodeType.SINK:
                sink = n
            if n.node_type == NodeType.HEAD:
                source = n
                
        return nx.all_simple_paths(self.G, source, sink)
    
    def get_topological_ordered_nodes(self):
        return list(nx.topological_sort(self.G))
    
    def get_executor_nodes(self):
        return [u for u, n in self.G.nodes(data=True) 
                if (u.node_type == NodeType.EXECUTER)]  
    
    def get_producer_nodes(self):
        return [u for u, n in self.G.nodes(data=True) 
                if (u.node_type == NodeType.PRODUCER)]      
    
    def get_host_code_relationship(self) -> dict:
        return self.current_host_code_dist
    
    def set_source(self, node:TopologyNode):
        self.Head = node
    
    def add_node(self, node:TopologyNode, in_topics, out_topics):
        self.G.add_node(node, in_topics=in_topics, out_topics=out_topics)

    def add_edge(self, from_node:TopologyNode, to_node:TopologyNode, topic):
        self.G.add_edge(from_node, to_node, topic=topic)
        
    def get_predecessors_from_name(self, name:str):
        crr_node = [n for n in self.G.nodes if n.name == name][0]
        return list(self.G.predecessors(crr_node))
        
    def get_predecessors(self, node:TopologyNode):
        return list(self.G.predecessors(node))
    
    def get_successors(self, node:TopologyNode):
        return list(self.G.successors(node))
            
    def get_nodes_from_input_topic(self, input_topic):
        return [u for u, n in self.G.nodes(data=True) 
                if (u.node_type == NodeType.PRODUCER or u.node_type == NodeType.EXECUTER) and input_topic in n['out_topics']]
            
    def get_nodes_from_out_topic(self, output_topic):
        return [u for u, n in self.G.nodes(data=True) 
                if (u.node_type == NodeType.EXECUTER) and  output_topic in n['in_topics']]
            
    def print_nodes(self):
        for u, n in self.G.nodes(data=True):
            print(u, " ", n)
            
    def get_network(self):
        return self.G

    def reset_visited_status(self):
        for u, n in self.G.nodes(data=True):
            u.processed = False

    # matplotlib is resource heavy        
    # def visualize_network(self):
    #     pos = nx.spring_layout(self.G)
        
    #     color_map = []
    #     for node, v in self.G.nodes(data=True):
    #         if node.node_type == NodeType.HEAD:
    #             color_map.append('pink')
    #         if node.node_type == NodeType.PRODUCER:
    #             color_map.append('green')
    #         if node.node_type == NodeType.EXECUTER:
    #             color_map.append('red')
    #         if node.node_type == NodeType.SINK:
    #             color_map.append('black')
        
    #     label_map = {}
    #     for node, v in self.G.nodes(data=True):
    #         if node.node_type == NodeType.HEAD:
    #             label_map[node] = node.name
    #         if node.node_type == NodeType.PRODUCER:
    #             label_map[node] = str(node.name) + " " + str(node.node_data.producer_name)
    #         if node.node_type == NodeType.EXECUTER:
    #             label_map[node] = str(node.name) + " " + str(node.node_data._settings.host_name)
    #         if node.node_type == NodeType.SINK:
    #             label_map[node] = node.name
        
    #     plt.figure(figsize=(10,10))
    #     nx.draw(
    #         self.G, edge_color='black', width=1, linewidths=1,
    #         node_size=500, 
    #         node_color=color_map,    
    #         # node_color='pink', 
    #         alpha=0.9,
    #         labels=label_map
    #     )
    #     plt.savefig("visuals/task_topology.png")
    #     plt.axis('off')
    #     plt.clf()
    #     plt.close()

    def get_bfs(self) -> nx.DiGraph:
        return nx.bfs_tree(self.G, self.Head)
    
    def print_graph_test(self):
        bfs = self.get_bfs()
        with open(self.graph_file, "a") as outfile:
            for node, v in bfs.nodes(data=True):
                outfile.write(node.to_text())
        outfile.close()
