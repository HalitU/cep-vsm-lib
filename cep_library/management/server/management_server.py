import pickle
import time
from datetime import datetime
from threading import Lock, Thread

import networkx as nx
import requests

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

import cep_library.configs as configs
from cep_library.cep.model.cep_task import CEPTask
from cep_library.management.distribution_algorithms.finalized_algorithms.genetic_algorithm import genetic_algorithm
from cep_library.management.distribution_algorithms.finalized_algorithms.random_distribution import random_distribution
from cep_library.management.distribution_algorithms.finalized_algorithms.greedy import \
    greedy
from cep_library.management.distribution_algorithms.finalized_algorithms.constrained_programming import \
    constrained_programming
from cep_library.management.distribution_algorithms.finalized_algorithms.complete_round_robin import \
    complete_round_robin
from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.task_alteration import TaskAlterationModel
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent

class ManagementServer:
    def __init__(self, app: FastAPI) -> None:        
        self.app: FastAPI = app
        
        self._host_name = configs.env_server_name

        # producer
        self._alteration_publisher = MQTTProducer(
            client_id=f"producer_{configs.kafka_resource_topics[0]}_{self._host_name}")

        # consumer which will listen the incoming resource consumptions
        self._resource_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_resource_topics[0]}_{self._host_name}",
            topic_name=configs.kafka_resource_topics,
            target=self.process_resource_data,
            target_args=None,
        )

        # worker records that sent request to this server
        self.w_ix = 1
        self.workers = {}
        self.tasks = []
        self.worker_lock = Lock()

        # raw producer records for managing them
        self.p_ix = 1
        self.producers = {}
        self.producer_ix = {}
        self.producer_lock = Lock()

        # current topology
        self.topology:Topology=None

        # Holding statistics
        self.statistic_records = {}
        self.producer_records = {}
        
        # Statistics files
        self.record_id_tracker = 1
        stat_headers = ResourceUsageRecord()
        
        # historic distribution
        self.distribution_history = {}
        self.last_min_cost = 1000000
        
        # self.stats_file = "stats/device_statistics_"+datetime.now().strftime("%m.%d.%Y_%H.%M.%S.%f")+".csv"
        self.stats_file = "stats/device_statistics.csv"
        with open(self.stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_device_stat_header()) + '\n')
        outfile.close()

        # self.task_stats_file =  "stats/task_statistics_"+datetime.now().strftime("%m.%d.%Y_%H.%M.%S.%f")+".csv"
        self.task_stats_file =  "stats/task_statistics.csv"
        with open(self.task_stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_task_stats_header()) + '\n')
        outfile.close()

        # self.task_topic_stats_file =  "stats/task_statistics_"+datetime.now().strftime("%m.%d.%Y_%H.%M.%S.%f")+".csv"
        self.task_topic_stats_file =  "stats/task_topic_stats_file.csv"
        with open(self.task_topic_stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_task_topic_stats_file_header()) + '\n')
        outfile.close()

        # self.server_stats_file =  "stats/server_statistics_"+datetime.now().strftime("%m.%d.%Y_%H.%M.%S.%f")+".csv"
        self.server_stats_file = "stats/server_statistics.csv"
        with open(self.server_stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_server_stat_header()) + '\n')
        outfile.close()

        # event execution distributions
        self.execution_distribution_file = "stats/execution_distribution_file.csv"
        execution_distribution_headers = ['row_id', 'current_date', 'sim_date', 'host_name', 'n_distribution', 'n_output_target']
        with open(self.execution_distribution_file, "w") as outfile:
            outfile.write(','.join(execution_distribution_headers) + '\n')
        outfile.close()

        self.stat_write_available = True

        # Indicates that the server is fully operational ready
        self.server_ready = True
        self.killed = False
        self.valid_stats_count = 0
        self.previous_alterations = []
        self.previous_producer_updates = []

        Thread(daemon=True, target=self.run).start()

        self.register_endpoints()

        print("[+] server initialized")

    def get_distribution_action(self):
        match configs.env_distribution_type:
            case 8:
                return greedy
            case 21:
                return complete_round_robin
            case 25:
                return constrained_programming
            case 26:
                return random_distribution
            case 31:
                return genetic_algorithm            

    def generate_data_task_graph(self) -> Topology:
        #region Topology
        # Think about the network topology
        self.topology = Topology()
         
        # Add head node for keeping DAG
        head = TopologyNode(node_type=NodeType.HEAD, node_data=None, name='start')
        self.topology.add_node(head, in_topics=None, out_topics=None)
        self.topology.set_source(head)
        
        # Add the raw data producers
        producer:RawSettings=None
        for _, producer in self.producers.items():
            producer_node = TopologyNode(node_type=NodeType.PRODUCER, node_data=producer, name=producer.raw_data_name)
            producer_output_topics = [p.output_topic for p in producer.output_topics]
            self.topology.add_node(producer_node, in_topics=None, out_topics=producer_output_topics)
            self.topology.add_edge(head, producer_node, None)
                
        # Add the code executors
        task:CEPTask=None
        for task in self.tasks:
            executor_node = TopologyNode(node_type=NodeType.EXECUTER, node_data=task, name=task._settings.action_name)
            input_topics = [topic.input_topic for topic in task._settings.required_sub_tasks]
            output_topics = [topic.output_topic for topic in task._settings.output_topics]
            self.topology.add_node(executor_node, in_topics=input_topics, out_topics=output_topics)
            
            # Find the connection via topic relationship and create the edge
            # This assumes DAG and starts searching from the head node
            # considering the network size is small it should be relatively fast.
            # If the source does not exists yet, then only node itself is created, other node
            # which is created can be joined later on.
            # First check if any of the nodes have one of the input topics as one of its output topic
            for topic in input_topics:
                source_nodes = self.topology.get_nodes_from_input_topic(topic)
                for source_node in source_nodes:
                    self.topology.add_edge(source_node, executor_node, topic)
            
            for topic in output_topics:
                target_nodes = self.topology.get_nodes_from_out_topic(topic)
                for target_node in target_nodes:
                    self.topology.add_edge(executor_node, target_node, topic)
        
        # Add the sink node to the nodes which has no successors
        sink = TopologyNode(node_type=NodeType.SINK, node_data=None, name='end')
        self.topology.add_node(sink, in_topics=None, out_topics=None)
        node:TopologyNode
        for u, _ in self.topology.G.nodes(data=True):
            if not self.topology.get_successors(u) and u is not sink:
                self.topology.add_edge(u, sink, topic=None)
        
        is_weakly_connected = nx.is_weakly_connected(self.topology.G)
        print("Is graph weakly connected: ", is_weakly_connected)
        is_directed = self.topology.G.is_directed()
        print("Is graph directed: ", is_directed)
        is_acyclic = nx.is_directed_acyclic_graph(self.topology.G)
        print("Is graph directed acyclic: ", is_acyclic)
        
        if not is_directed or not is_acyclic or not is_weakly_connected:
            print("Graph needs to be directed acyclic and all nodes should connect to each other!")
            return None
        
        return self.topology
        #endregion Topology        

    # periodically evaluates the statistics and
    # decides if an update is required for tasks or data flow etc.
    # For a record, only active tasks in that server at that point are 
    #   included in the task_records field. statistic_records field has data for all executions
    #   since the beginning of the device life.
    # If there is a difference between two records in terms of total executions for a task,
    #   it means that specific task is executed throughout this stat collection period.
    # TODO: all tasks should be activated on at least one server otherwise everything will die
    #   at some random point.
    def periodic_evaluation(self):
        evaluation_period = configs.evaluation_period
        
        while not self.killed:
            self.periodic_evaluation_detail()
            time.sleep(evaluation_period)

        print("[+] server killing periodic evaluation...")

    def periodic_evaluation_detail(self):
        if configs.EVAL_ACTIVE == 0:
            print("Evaluation is not active yet...")
            return
        
        self.worker_lock.acquire()
        # Do some evaluation
        print("evaluating...")
        current_server_time = time.perf_counter_ns()

        # Get the statistics from the workers
        workers_to_be_deleted = []
        for worker_conn in self.workers:
            try:
                worker_port = self.workers[worker_conn]['worker_port']
                worker_url = 'http://'+worker_conn+':'+str(worker_port)+'/statistics_report/'
                print("[STATS] Server requesting resource data, URI is: ", worker_url)
                resource_consumption = requests.get(url=worker_url, timeout=60.0)
                self.process_resource_data(resource_consumption.content, None)
            # Remove the worker from the distribution pool, it can be added if the worker
            # attempts to reconnect again with a heartbeat
            except Exception as e:
                print("Error: ", str(e))
                if worker_conn not in workers_to_be_deleted:
                    print("Current workers: ", [*self.workers])
                    workers_to_be_deleted.append(worker_conn)
        
        # for worker_to_be_deleted in workers_to_be_deleted:
        #     if worker_to_be_deleted in self.workers:
        #         print("Deleting worker: ", worker_to_be_deleted)
        #         del self.workers[worker_to_be_deleted]
        
        # Get the statistics from the raw data producers
        for producer_node in self.producers:
            try:
                rawSetting:RawSettings = self.producers[producer_node]
                producer_url = 'http://'+rawSetting.producer_name+':'+str(rawSetting.producer_port)+'/raw_statistics_report/'+rawSetting.raw_data_name+"/"
                print("[STATS] Server requesting resource data, URI is: ", producer_url)
                producer_stats = requests.get(url=producer_url, timeout=60.0)
                prod_stat_msg = pickle.loads(producer_stats.content)
                print("Collected stats from the producer: ", prod_stat_msg)
                self.producer_records[rawSetting.raw_data_name] = prod_stat_msg
            # Remove the worker from the distribution pool, it can be added if the worker
            # attempts to reconnect again with a heartbeat
            except Exception as e:
                print("[!!!!!!!!!!!!] ERROR: ", str(e))
                pass
        
        print(f"Current valid evaluation count: {self.valid_stats_count}, evaluation stops after: {configs.USE_HISTORIC_DIST_AFTER_N}")
        if self.valid_stats_count >= configs.USE_HISTORIC_DIST_AFTER_N and configs.USE_HISTORIC_DIST_AFTER_N > 0:
            print("Evaluation completed USING PREVIOUS ITEMS")
            cep_evaluation_time = (time.perf_counter_ns() - current_server_time) / 1000000.0
            self.print_distributions(self.previous_alterations, self.previous_producer_updates)
            self.print_server_stats(datetime.utcnow().strftime("%m.%d.%Y %H.%M.%S.%f") + "," + str(cep_evaluation_time) + "," + str(0.0))
            self.worker_lock.release()
            return        
        
        # Get the current DAG topology
        topology = self.generate_data_task_graph()
        
        if topology is None:
            print("Topology is not valid...")
            self.worker_lock.release()
            return              
        
        # Get the distribution action
        dist_action = self.get_distribution_action()
        
        current_server_time_for_algorithm_only = time.perf_counter_ns()
        # Get alteration requests
        if configs.env_distribution_type in [15, 16, 17, 18, 19, 20, 21, 22, 31]:
            alteration_requests, producer_updates, self.distribution_history = dist_action(self.workers, topology, self.statistic_records, self.producer_records, self.distribution_history)
        elif configs.env_distribution_type in [23, 24, 25]:
            alteration_requests, producer_updates, self.distribution_history, self.last_min_cost = dist_action(self.workers, topology, self.statistic_records, self.producer_records, self.distribution_history, self.last_min_cost, self.valid_stats_count)
        else:
            alteration_requests, producer_updates = dist_action(self.workers, topology, self.statistic_records, self.producer_records)
        current_evaluation_time_for_algorithm_only = (time.perf_counter_ns() - current_server_time_for_algorithm_only) / 1000000.0
                
        activated_task_count = sum([1 if alteration_req.activate == True else 0 for alteration_req in alteration_requests])
        if len(self.topology.get_executor_nodes()) != activated_task_count:
            print("Not all tasks can be activated at the moment, activated task count: ", activated_task_count)
            self.worker_lock.release()
            return

        if self.valid_stat_exists(self.statistic_records):
            self.valid_stats_count += 1
                
        self.previous_alterations = alteration_requests
        self.previous_producer_updates = producer_updates
                
        # Producer updates
        producer_request: RawUpdateEvent=None
        for producer_request in producer_updates:
            print("Producing producer alteration request to: ", producer_request.producer_name)
            msg = pickle.dumps(producer_request)
            self._alteration_publisher.produce(
                configs.kafka_producer_topics[0],
                msg,
                on_delivery=self.ack
            )                        
                
        # produce updates
        alteration_request:TaskAlterationModel=None
        for alteration_request in alteration_requests:
            msg = pickle.dumps(alteration_request)
            self._alteration_publisher.produce(
                configs.kafka_alteration_topics[0],
                msg,
                on_delivery=self.ack,
            )                 

        print("Evaluation completed...")
        cep_evaluation_time = (time.perf_counter_ns() - current_server_time) / 1000000.0
        self.print_distributions(alteration_requests, producer_updates)
        self.print_server_stats(datetime.utcnow().strftime("%m.%d.%Y %H.%M.%S.%f") + "," + str(cep_evaluation_time) + "," + str(current_evaluation_time_for_algorithm_only))
        self.worker_lock.release()        

    def valid_stat_exists(self, node_records:dict):
        for _, record in node_records.items():
            # for this host
            if record:
                resource_consumption: ResourceUsageRecord = record
                for stat_record in resource_consumption.statistic_records:
                    if stat_record.crr_cpu_usage_stats.count == 0.0:
                        return False
        return True

    #region Helpers
    def reset_stat_files(self):
        print("Resetting statistic files for future simulations...")
        self.stat_write_available = False
        # Wait a bit so that if there are other writings they will finish
        time.sleep(10.0)
        
        # Rest collected statistics
        self.statistic_records = {}
        
        stat_headers = ResourceUsageRecord()
        
        with open(self.stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_device_stat_header()) + '\n')
        outfile.close()

        with open(self.task_stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_task_stats_header()) + '\n')
        outfile.close()

        with open(self.task_topic_stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_task_topic_stats_file_header()) + '\n')
        outfile.close()

        with open(self.server_stats_file, "w") as outfile:
            outfile.write(','.join(stat_headers.get_server_stat_header()) + '\n')
        outfile.close()        
        self.stat_write_available = True
    
    def print_distributions(self, alteration_requests, producer_updates):
        print("-----------------------------------------------------------")
        row_id = self.record_id_tracker
        current_date = datetime.utcnow().strftime("%m.%d.%Y %H.%M.%S.%f")
        # ['row_id', 'current_date', 'sim_date', 'host_name', 'n_distribution', 'n_output_target']
        # event execution distributions
        with open(self.execution_distribution_file, "a") as outfile:
            hosts = {}
            alteration_request:TaskAlterationModel=None
            for alteration_request in alteration_requests:
                if alteration_request.activate:
                    # executions
                    if alteration_request.host in hosts:
                        if 'n_distribution' in hosts[alteration_request.host]:
                            hosts[alteration_request.host]['n_distribution'] += 1
                        else:
                            hosts[alteration_request.host]['n_distribution'] = 1
                    else:
                        hosts[alteration_request.host] = {}
                        hosts[alteration_request.host]['n_distribution'] = 1
                    
                    # outputs
                    for out in alteration_request.cep_task._settings.output_topics:
                        if out.target_database in hosts:
                            if 'n_output_target' in hosts[out.target_database]:
                                hosts[out.target_database]['n_output_target'] += 1
                            else:
                                hosts[out.target_database]['n_output_target'] = 1
                        else:
                            hosts[out.target_database] = {}
                            hosts[out.target_database]['n_output_target'] = 1
            
            # raw data producer alterations        
            producer_request: RawUpdateEvent=None
            for producer_request in producer_updates:            
                for out in producer_request.output_topics:
                    if out.target_database in hosts:
                        if 'n_output_target' in hosts[out.target_database]:
                            hosts[out.target_database]['n_output_target'] += 1
                        else:
                            hosts[out.target_database]['n_output_target'] = 1
                    else:
                        hosts[out.target_database] = {}
                        hosts[out.target_database]['n_output_target'] = 1
            
            if hosts:
                for host in hosts:
                    crr_n_dist = 0
                    if 'n_distribution' in hosts[host]:
                        crr_n_dist = hosts[host]['n_distribution']
                    crr_n_out = 0
                    if 'n_output_target' in hosts[host]:
                        crr_n_out = hosts[host]['n_output_target']
                        
                    to_out = [str(row_id), current_date, datetime.now().strftime("%m.%d.%Y %H.%M.%S.%f"), host, str(crr_n_dist), str(crr_n_out)]
                    outfile.write(','.join(to_out) + '\n')
                
    def print_device_stats(self, json_str):
        if self.stat_write_available == False: return
        with open(self.stats_file, "a") as outfile:
            outfile.write(json_str + '\n')
        outfile.close()

    def print_task_stats(self, json_str):
        if self.stat_write_available == False: return
        with open(self.task_stats_file, "a") as outfile:
            outfile.write(json_str + '\n')
        outfile.close()
        
    def print_task_topic_stats(self, json_str):
        if self.stat_write_available == False: return
        with open(self.task_topic_stats_file, "a") as outfile:
            outfile.write(json_str + '\n')
        outfile.close()
        
    def print_server_stats(self, json_str):
        if self.stat_write_available == False: return
        with open(self.server_stats_file, "a") as outfile:
            outfile.write(json_str + '\n')
        outfile.close()        

    def run(self):
        # Start consuming in the background
        Thread(daemon=True, target=self._alteration_publisher.run).start()
        Thread(daemon=True, target=self._resource_consumer.run).start()

        # start the periodic process for sending resource consumptions
        Thread(daemon=True, target=self.periodic_evaluation).start()

    def ack(self, err, msg):
        pass

    """ Register REST endpoints required for initial cluster setup or when
    a new workers needs to connect to the cluster. """

    def register_endpoints(self):
        @self.app.get("/download_device_stats/")
        def download_device_stats():
            return FileResponse(path=self.stats_file, filename="device_stats.csv")
        
        @self.app.get("/download_task_stats/")
        def download_task_stats():
            return FileResponse(path=self.task_stats_file, filename="task_stats.csv")
    
        @self.app.get("/download_server_stats/")
        def download_server_stats():
            return FileResponse(path=self.server_stats_file, filename="server_stats.csv")
        
        @self.app.get("/get_server_action_file/")
        def server_ready(file_path:str):
            if '.' not in file_path:
                file_name = file_path.split('/')[-1]+'.py'
                file_path = file_path.replace('.', '/')+'.py'
            else:
                file_name = file_path.split('/')[-1]
            return FileResponse(path=file_path, filename=file_name)
        
        @self.app.post("/raw_producer/")
        async def raw_producer(raw_settings:Request):
            request_data:bytes = await raw_settings.body()
            raw_settings: RawSettings = pickle.loads(request_data)
            if raw_settings.raw_data_name not in self.producers:
                self.producer_lock.acquire()
                print(f"Registering a raw data producer with new id: {self.p_ix}.")
                self.producers[raw_settings.raw_data_name] = raw_settings
                self.producer_ix[raw_settings.raw_data_name] = self.p_ix
                self.p_ix += 1
                print(f"Raw producer registered with id: {self.p_ix}")
                self.producer_lock.release()
            return self.server_ready
        
        @self.app.get("/server_ready/")
        def server_ready(worker_name: str, worker_port: int, worker_cpu_limit:float=0.0):
            # print("Checking if the server is ready for worker: ", worker_name)
            self.worker_lock.acquire()
            
            if worker_name not in self.workers:
                self.workers[worker_name] = {
                    'worker_port': worker_port, 
                    'worker_cpu_limit': worker_cpu_limit,
                    "w_ix": self.w_ix
                    }
                print("Added new worker: ", worker_name, " with id: ", self.w_ix)
                self.w_ix += 1
            
            self.worker_lock.release()
            return self.server_ready

        @self.app.get("/get_topology/")
        def get_topology(file_path:str="visuals/task_topology.png", filename:str="task_topology.png"):
            return FileResponse(path=file_path, filename=filename)

        @self.app.get("/get_topology_text/")
        def get_topology(file_path:str="visuals/graph_output.txt", filename:str="graph_output.txt"):
            return FileResponse(path=file_path, filename=filename)

        @self.app.on_event("startup")
        async def startup_event():
            print("starting up the web server...")


        @self.app.on_event("shutdown")
        def shutdown_event():
            print("Shutting down")
            self.stop_management()

        @self.app.get("/download_all_statistics/")
        def statistics():
            # Zip the statistics directory
            return FileResponse(path='current_run_results.zip', filename="current_run_results.zip")

    # there might be a bug about the CEPTask receiving from here
    def process_resource_data(self, msg: bytes, args):
        # print("[STATS] Server received resource data.")
        # print("hm: ", msg)
        resource_consumption: ResourceUsageRecord = pickle.loads(msg)
        # print("cpu: %s, ram: %s " % (resource_consumption.cpu, resource_consumption.ram))
        # action_names = [
        #     task_record.cep_task._settings.action_name
        #     for task_record in resource_consumption.task_records
        # ]
        # print("actions %s for the host %s" % (action_names, resource_consumption.host))
                
        # print("hm")
        # print(resource_consumption.__dict__)
        
        # record the latest statistics for this worker host
        self.statistic_records[resource_consumption.host] = resource_consumption
        
        # current server time is useful for determining when the statistics are recorded
        current_server_time = datetime.now().strftime("%m.%d.%Y %H.%M.%S.%f")
        
        # Recording the statistics for device and the tasks executed within the device.
        device_stats = resource_consumption.get_device_stats(self.record_id_tracker)
        device_stats.append(current_server_time)
        self.print_device_stats(','.join(device_stats))
        
        print("------------------------------------------------------")
        print("[SERVER] Written the device stats...")
        # print(resource_consumption.__dict__)
        
        for task_stat in resource_consumption.get_task_stats(self.record_id_tracker):
            print(task_stat)
            crr_task_stat= task_stat
            crr_task_stat.append(current_server_time)
            self.print_task_stats(','.join(crr_task_stat))
        
        for task_topic_stat in resource_consumption.get_task_topics_stats(self.record_id_tracker):
            crr_task_topic_stat = task_topic_stat
            crr_task_topic_stat.append(current_server_time)
            self.print_task_topic_stats(','.join(crr_task_topic_stat))
        
        print("------------------------------------------------------")            
        # Increment the statistics tracker
        self.record_id_tracker+=1
        # print("[SERVER] processed resource usage.")

    def stop_management(self):
        self._resource_consumer.close()
        self._alteration_publisher.close()
        self.killed = True

    def register_task(self, task:CEPTask) -> None:
        self.tasks.append(task)
    #endregion Helpers