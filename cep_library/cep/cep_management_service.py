import asyncio
import importlib
import os
import pickle
import sys
import threading
import time
from datetime import datetime, timedelta
from threading import Lock, Thread
from typing import List

import psutil
import pymongo
import requests
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import Response
from pympler import asizeof

import cep_library.configs as configs
from cep_library.cep.model.cep_task import CEPTask, EventModel, InputTopicCounter, MongoQuery, RequiredInputTopics, \
    RequiredOutputTopics
from cep_library.data.base_database_management import BaseDatabaseManagement
from cep_library.data.database_factory import DatabaseFactory
from cep_library.data.model.database_info_model import DataCarryModel
from cep_library.management.model.resource_usage import ResourceUsageRecord, TaskRecords
from cep_library.management.model.statistics_records import SingleStatisticsRecord, StatisticRecord, \
    TopicStatisticDetail
from cep_library.management.model.task_alteration import DataMigrationModel, TaskAlterationModel
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer


# Monitoring bandwidth
class CEPManagementService:
    def __init__(self, host_name, app: FastAPI, 
                 db:BaseDatabaseManagement, 
                 threads_for_at_least_once_trigger) -> None:
        self.send_heartbeat(host_name)
        
        self._host_name = host_name
        self.database_management_one = db
        self._resource_publisher = MQTTProducer(
            f"producer_{configs.kafka_alteration_topics[0]}_{host_name}")
        self._alteration_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_alteration_topics[0]}_{host_name}",
            topic_name=configs.kafka_alteration_topics,
            target=self.process_consumer_alteration
        )
        self.killed = False
        self.executed_task_count = 0
        self.processed_msg_count = 0
        self.carried_data_count = 0
        self.deleted_data_count = 0
        self.actions = {}
        self.actions_to_activate = []
        self.data_to_migrate = List[DataMigrationModel]
        self.action_statistics = {}
        
        self.required_manipulation = False
        self.threads_for_at_least_once_trigger = threads_for_at_least_once_trigger
        
        self.pid = os.getpid()
        self.process = psutil.Process(self.pid)
        self.process.cpu_percent()
        
        # for self cpu calculation
        self.host_cpu_start_date = datetime.utcnow()
        if configs.OS_TYPE == 0:
            self.host_cpu_start_hertz = float(os.popen('cat /sys/fs/cgroup/cpuacct/cpuacct.usage').read())
        elif configs.OS_TYPE == 1:
            cpu_usage_times = os.popen('cat /sys/fs/cgroup/cpu.stat').readlines()
            self.host_cpu_start_hertz = float(cpu_usage_times[0].split(" ")[1])
        
        # https://stackoverflow.com/questions/51248144/getting-cpu-and-memory-usage-of-a-docker-container-from-within-the-dockerized-ap
        
        
        self.current_bandwdith_usage = 0.0
                
        Thread(daemon=True, target=self.run).start()
        
        self.register_action_file_service(app)
        
        # self.action_debug_list = ["audio_task", "alarm_task"]
        self.action_debug_list = ["audio_task"]
        print ("[*] client initialized.")

    def register_action_file_service(self, app: FastAPI):
        @app.get("/get_action_file/")
        def server_ready(file_path:str, file_name):
            file_path = file_path.replace('.', '/')+'.py'
            return FileResponse(path=file_path, filename=file_name)
        
        @app.get("/stop_execution/")
        def stop_execution():
            self.stop_management()
            return Response(status_code=200)
        
        # https://stackoverflow.com/questions/71794990/fast-api-how-to-return-a-str-as-json
        @app.get("/statistics_report/")
        def statistics_report():
            print("Server requested statistics...")            
            encoded = self.resource_usage()
            
            for action_statistic in self.action_statistics:
                self.action_statistics[action_statistic].reset_stats()            
        
            print("Statistics sent to management device...")
            
            # start the required manipulator if not started yet...
            if self.required_manipulation == False:
                print("[CEP Manager] Starting the required manipulator...")
                self.required_manipulation = True
                
                # Triggers functions in threads for executing when the device statistics are called at least once. For utilitiy purposes.
                for at_least_once_threads in self.threads_for_at_least_once_trigger:
                    Thread(daemon=True, target=at_least_once_threads).start()
            
            return Response(content=encoded, media_type="application/json")

    # region managing the service
    def run(self):
        # start the storage management
        Thread(daemon=True, target=self.database_management_one.run).start()

        # Start consuming alteration requests in the background
        Thread(daemon=True, target=self._resource_publisher.run).start()
        Thread(daemon=True, target=self._alteration_consumer.run).start()

        # Start consuming alteration requests in the background
        Thread(daemon=True, target=self.activate_waiting_actions).start()
        
        # Start consuming alteration requests in the background
        Thread(daemon=True, target=self.periodic_heartbeat_to_server).start()

        # Start consuming alteration requests in the background
        Thread(daemon=True, target=self.bandwidth_usage).start()

        # We should not need anything new regards to management after this point
        print("Client initialized")

    def bandwidth_usage(self):
        old_value = 0.0
        while not self.killed:
            net_counter = psutil.net_io_counters()
            new_value = net_counter.bytes_sent + net_counter.bytes_recv

            if old_value:
                bw_kbit = (new_value - old_value)/1024.*8
                self.current_bandwdith_usage = bw_kbit
                
            old_value = new_value

            time.sleep(1)        

    # Periodically sends the ready heartbeat to the server
    def periodic_heartbeat_to_server(self):
        while not self.killed:
            self.send_heartbeat(self._host_name)
            time.sleep(1)
        
    def send_heartbeat(self, host_name):
        # wait until server sends ready response
        ready = False or configs.env_server_name == 'localhost'
        server_url = 'http://'+configs.env_server_name+':'+str(configs.env_server_port)+'/server_ready/'
        server_ready_params = {
                'worker_name': host_name,
                'worker_port': configs.env_host_port,
                'worker_cpu_limit': configs.CP_CPU_LIMIT
            }
        # print ('[*] server url is: ', server_url)
        while not ready:
            try:
                ready = requests.get(server_url, params=server_ready_params)
            except Exception as e:
                print ("[*] error during get request to server: ", e)
            # retry after 1 second
            time.sleep(1)
        # print ('[*] received ready from server')          

    def stop_management(self):
        self._resource_publisher.close()
        self._alteration_consumer.close()
        self.database_management_one.close()

        # do not forget to close consumer/producer of each action
        for action in self.actions:
            for consumer in self.actions[action]["consumer"]:
                consumer.close()
            for producer in self.actions[action]["producer"]:
                producer.close()
            for database in self.actions[action]["databases"]:
                database.close()

        self.killed = True

    def ack(self, err, msg):
        self.processed_msg_count+=1
        pass

    # Periodically activates the actions from main thread
    def activate_waiting_actions(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while not self.killed:
            # Action activation
            action_count = len(self.actions_to_activate)
            if action_count > 0:
                # print("Number of actions to activate: ", action_count)
                pass
            action:CEPTask=None
            for action in self.actions_to_activate[:]:
                # print("Activating: ",  action._settings.action_name)
                self.add_action(action)
                self.actions_to_activate.remove(action)
                        
            time.sleep(1)
                
    def resource_usage(self)->ResourceUsageRecord:
        print("----------------------------------------------------")
        # CPU calculations
        cpu_start_date = datetime.utcnow()
        if configs.OS_TYPE == 0:
            cpu_start_hertz = float(os.popen('cat /sys/fs/cgroup/cpuacct/cpuacct.usage').read())
            cpu_usage_times = os.popen('cat /sys/fs/cgroup/cpuacct/cpuacct.stat').readlines()
            cpu_usage_hertz_total = 0.0
            for cpu_usage_time in cpu_usage_times:
                cpu_usage_hertz_total += float(cpu_usage_time.split(" ")[1])
        # due to cpuacct problem on rpi4...
        elif configs.OS_TYPE == 1:
            cpu_usage_times = os.popen('cat /sys/fs/cgroup/cpu.stat').readlines()
            cpu_usage_hertz_total = 0.0
            counter = 0
            for cpu_usage_time in cpu_usage_times:
                if counter == 0:
                    cpu_start_hertz = float(cpu_usage_time.split(" ")[1])
                elif counter == 1 or counter == 2:
                    cpu_usage_hertz_total += float(cpu_usage_time.split(" ")[1])
                counter += 1
        
        while True:
            try:    
                process_threads_sum = sum([t.system_time + t.user_time for t in self.process.threads()])
                break
            except Exception as e:
                print("[CEP MANAGER] Exception while accessing self process threads: ", str(e))

        print("Current threads sum: ", process_threads_sum)
            
        print(f"cpu_start_date {cpu_start_date}")
        print(f"cpu_start_time {cpu_start_hertz}")
        print(f"cpu_usage_time_total {cpu_usage_hertz_total}")
        
        # get the resource usage stats
        resource_usage = ResourceUsageRecord()
        resource_usage.host = self._host_name
        resource_usage.record_date = datetime.utcnow()
        resource_usage.cpu = ((cpu_start_hertz - self.host_cpu_start_hertz) / 1000000000.0) / (cpu_start_date - self.host_cpu_start_date).seconds
        resource_usage.ram = self.process.memory_percent()
        resource_usage.task_records = []
        resource_usage.statistic_records = []

        # get execution stats
        resource_usage.executed_task_count=self.executed_task_count
        resource_usage.processed_msg_count=self.processed_msg_count
        resource_usage.carried_data_count = self.carried_data_count
        resource_usage.deleted_data_count = self.deleted_data_count

        # get the statistics of each task
        for action_name in self.actions:
            action: CEPTask = self.actions[action_name]["action"]
            task_record = TaskRecords()
            # task_record.cep_task = action            
            resource_usage.task_records.append(task_record)

        # Is it wise to take only the currently active ones?
        action_name:str
        action_statistic: StatisticRecord
        sum_of_action_threads = 0.0
        for action_name, action_statistic in self.action_statistics.items():
            if action_name in self.actions:
                action: CEPTask = self.actions[action_name]["action"]
                sum_of_action_threads += action_statistic.crr_cpu_usage_stats.total
            
                cpu_thread_elapsed_seconds = (cpu_start_date - action.cpu_start_date).seconds
                if cpu_thread_elapsed_seconds > 0:
                    cpu_percentage = ((cpu_start_hertz - action.cpu_start_hertz) / 1000000000.0) / cpu_thread_elapsed_seconds
                else:
                    cpu_percentage = 0.0
                cpu_hertz_percentage = ((action_statistic.crr_cpu_usage_stats.total) * 1.0 / ((cpu_usage_hertz_total - action.cpu_start_usage_hertz_total) / 1000000000.0))
                thread_finalized_percentage = cpu_hertz_percentage * cpu_percentage
                                    
                # This percentage is calculated from the cpus, lets say we limited cpus to 1 in docker, it will be max around 1.0~1.1
                print(f"Current percentage consumption: ", cpu_percentage)
                print(f"Calculated averge cpu percentage for action: {action._settings.action_name} is: {action_statistic.crr_cpu_usage_stats.avg}, total usage is: {action_statistic.crr_cpu_usage_stats.max}")
                
                # update the new start positions for the cpu calculations in case this device keeps the current action in the next cycle
                action.cpu_start_date = cpu_start_date
                action.cpu_start_hertz = cpu_start_hertz
                action.cpu_start_usage_hertz_total = cpu_usage_hertz_total
                            
                resource_usage.statistic_records.append(action_statistic)
        
        print(f"Total usage of cpu by action threads is: ", sum_of_action_threads)
        print("----------------------------------------------------")        
        
        finalized_stats = pickle.dumps(resource_usage)
                
        return finalized_stats
   
    # tasks, data storage and flows are manipulated here
    def process_consumer_alteration(self, msg: bytes, args):
            self.processed_msg_count+=1
            # data = json.loads(msg.decode())
            # alteration_request = TaskAlterationModel(**data)
            alteration_request:TaskAlterationModel = pickle.loads(msg)
            # print(alteration_request.__dict__)

            # do the task update if needed
            if (
                alteration_request.host != None
                and alteration_request.host == self._host_name
            ):
                # print(
                #     self._host_name, " received an alteration request: ", alteration_request,
                #     " for job: ", alteration_request.job_name,
                #     " to activate: ", alteration_request.activate,
                #     " current actions: ", [*self.actions]
                # )
                if (
                    alteration_request.activate == False
                    and not alteration_request.job_name is None
                    and alteration_request.job_name in self.actions
                ):
                    self.remove_action(alteration_request.job_name)
                    
                    # carry over last T data from previous db to new one
                    if alteration_request.migration_requests:
                        for migration_request in alteration_request.migration_requests:
                            
                            # create collection with index if it does not exist in target database!
                            self.database_management_one.create_collection(migration_request.target_host, migration_request.topic, 
                                                                           configs.collection_lifetime)
                            
                            if configs.data_carry_enabled:
                                query = MongoQuery()
                                query.sort = [("createdAt", pymongo.DESCENDING)]
                                query.within_timedelta = timedelta(seconds=configs.data_carry_window)
                                query.columns = { "_id": 1, "data": 1, "initDate": 1 }
                                
                                data_carry_model = DataCarryModel()
                                data_carry_model.source_host = migration_request.current_host
                                data_carry_model.target_host = migration_request.target_host
                                data_carry_model.collection = migration_request.topic
                                data_carry_model.remove_source = True
                                data_carry_model.query = query
                                
                                fin_deleted_count, fin_migrated_count = self.database_management_one.carry_data(data_carry_model)

                                self.carried_data_count += fin_migrated_count
                                self.deleted_data_count += fin_deleted_count
                            
                elif (
                    alteration_request.activate == True
                    and not alteration_request.job_name is None
                    and not alteration_request.job_name in self.actions
                ):
                    self.actions_to_activate.append(alteration_request.cep_task)
                elif alteration_request.activate == True and alteration_request.job_name in self.actions:
                    activated_action_to_manipulate:CEPTask = self.actions[alteration_request.job_name]["action"]
                    
                    old_topic_dbs = {ot.output_topic:ot.target_database for ot in activated_action_to_manipulate._settings.output_topics}                    
                    
                    # make sure any possible new target databases have the topics with indices available
                    output_topic:RequiredOutputTopics
                    for output_topic in alteration_request.cep_task._settings.output_topics:
                        self.database_management_one.create_collection(
                            host_name = output_topic.target_database,
                            collection_name = output_topic.output_topic,
                            ttl_duration = configs.collection_lifetime
                        )
                    
                        # Carry any existing data to new location, in other words this device might still have the action
                        # but write its output to someplace else!    
                        if output_topic.target_database != old_topic_dbs[output_topic.output_topic]:
                            if configs.data_carry_enabled:
                                query = MongoQuery()
                                query.sort = [("createdAt", pymongo.DESCENDING)]
                                query.within_timedelta = timedelta(seconds=configs.data_carry_window)
                                query.columns = { "_id": 1, "data": 1, "initDate": 1 }
                                
                                data_carry_model = DataCarryModel()
                                data_carry_model.source_host = old_topic_dbs[output_topic.output_topic]
                                data_carry_model.target_host = output_topic.target_database
                                data_carry_model.collection = output_topic.output_topic
                                data_carry_model.remove_source = True
                                data_carry_model.query = query
                                
                                fin_deleted_count, fin_migrated_count = self.database_management_one.carry_data(data_carry_model)

                                self.deleted_data_count += fin_deleted_count
                                self.carried_data_count += fin_migrated_count
                    
                    # update the settings
                    activated_action_to_manipulate._settings = alteration_request.cep_task._settings
        # except Exception as e:
        #     print("[ERROR] consumer alteration error due to: ", e)

    # endregion managing the service

    # region managing the events

    # executes the action on consuming a message from topic
    def execute_action(self, msg, action_msg_params):
        action: CEPTask
        triggered_topic_name:str
        database_connection:BaseDatabaseManagement
        stat_lock:Lock
        statistic_record:StatisticRecord
        host_name:str
        action_producer:MQTTProducer
        input_topic_counter:InputTopicCounter
        action, triggered_topic_name, database_connection, stat_lock, statistic_record, host_name, action_producer, input_topic_counter = action_msg_params
        initDates = []
        try:
            # Statistics
            current_time = time.perf_counter_ns()
            data_read_time = 0.0
            data_write_time = 0.0
            data_delete_time = 0.0
            event_publish_time = 0.0
            total_elapsed = 0.0
            task_execution_time = 0.0
            in_data_byte = 0.0
            out_data_byte = 0.0
            current_in_topic_records = {}
            in_local_data_byte = 0.0
            in_remote_data_byte = 0.0
            out_local_data_byte = 0.0
            out_remote_data_byte = 0.0
            
            # get the data
            data = None
            em_request:EventModel = pickle.loads(msg)
            
            data_lifetime_status_type = None
            if configs.AGGREGATE_ENABLED == 0:
                
                # get all latest ids if available
                _ttl = em_request.event_date + timedelta(milliseconds=configs.CONSTRAINED_TTL)
                
                id_list, id_data_list, available, data_lifetime_status_type = input_topic_counter.update_topic_counter(triggered_topic_name, em_request.data_id, _ttl, em_request.data)
                
                if available == False:
                    # miss
                    stat_lock.acquire()
                    statistic_record.update_topic_stats(data_lifetime_status_type, triggered_topic_name, False)
                    statistic_record.miss_count += 1
                    stat_lock.release()
                    return
                
                # hit
                required_sub_task:RequiredInputTopics=None
                datasets = {}
                for required_sub_task in action._settings.required_sub_tasks:
                    
                    current_in_topic_read_start_time = time.perf_counter_ns()
                    
                    # print(required_sub_task.input_topic)
                    sub_data_id = id_list[required_sub_task.input_topic]
                    if configs.DISABLE_VSM == 1:
                        print("Device is getting data from app memory!!")
                        sub_task_data = id_data_list[required_sub_task.input_topic]
                    else:
                        sub_task_data = database_connection.find_one(
                            sub_data_id,
                            required_sub_task.input_topic,
                            required_sub_task.stored_database,
                        )

                    # print(f"{action._settings.action_name} sub_task_data length: ", len(sub_task_data))
                    if sub_task_data:
                        value_matrix = {}
                        decoded_data = pickle.loads(sub_task_data['data'])
                        init_dates = sub_task_data['initDate']
                        for in_d in init_dates:
                            initDates.append(in_d)
                        
                        # print(f"{action._settings.action_name} decoded data: ", decoded_data)
                        for key, value in decoded_data.items():
                            if key in value_matrix:
                                if isinstance(value, list):
                                    for v in value:
                                        value_matrix[key].append(v)
                                else:
                                    value_matrix[key].append(value)
                            else:
                                if isinstance(value, list):
                                    value_matrix[key] = value
                                else:
                                    value_matrix[key] = [value]
                        # print(f"{action._settings.action_name} value matrix finalized lengths: ", [len(vm) for _, vm in value_matrix.items()])
                        is_local = False
                        for key, val in value_matrix.items():
                            if required_sub_task.stored_database == host_name:
                                is_local = True
                                in_local_data_byte += asizeof.asizeof(val)
                            else:
                                is_local = False
                                in_remote_data_byte += asizeof.asizeof(val)
                            
                            datasets[key] = val
                        # print("value_matrix: ", value_matrix)
                        crr_tsd = TopicStatisticDetail()
                        crr_tsd.data_process_time = (time.perf_counter_ns() - current_in_topic_read_start_time) / 1000000.0
                        crr_tsd.data_size = asizeof.asizeof(value_matrix) / 1024.0
                        crr_tsd.local = is_local
                        
                        current_in_topic_records[required_sub_task.input_topic] = crr_tsd
                    else:
                        print("No data to process stopping non-aggregate execution for: ", action._settings.action_name)
                        # miss due to VSM deadline
                        stat_lock.acquire()
                        statistic_record.update_topic_stats(data_lifetime_status_type, triggered_topic_name, False)
                        statistic_record.miss_count += 1
                        stat_lock.release()
                        return
                    
                if not datasets:                        
                    # miss due to VSM deadline
                    stat_lock.acquire()
                    statistic_record.update_topic_stats(data_lifetime_status_type, triggered_topic_name, False)
                    statistic_record.miss_count += 1
                    stat_lock.release()
                    return
                
                data = datasets
                # print("query data is: ", data)
            elif configs.AGGREGATE_ENABLED == 1:
                # querying instead of last_id control, when as much data as possible should be read is needed
                required_sub_task:RequiredInputTopics=None
                datasets = {}
                for required_sub_task in action._settings.required_sub_tasks:
                    
                    current_in_topic_read_start_time = time.perf_counter_ns()
                    
                    current_query = action._settings.query
                    current_query.limit = 0 if required_sub_task.is_image_read == False else current_query.limit
                    
                    sub_task_data = database_connection.query(
                        None,
                        action._settings.query,
                        required_sub_task.input_topic,
                        required_sub_task.stored_database,
                    )
                    
                    if len(sub_task_data) > 0 and (configs.MIN_REQUIRED_ACTIVE_VAR <= len(sub_task_data) or required_sub_task.is_image_read):
                        value_matrix = {}
                        for sub_data in sub_task_data:
                            decoded_data = pickle.loads(sub_data['data'])
                            init_dates = sub_data['initDate']
                            for in_d in init_dates:
                                initDates.append(in_d)
                            # print(f"{action._settings.action_name} decoded data: ", decoded_data)
                            for key, value in decoded_data.items():
                                if key in value_matrix:
                                    if isinstance(value, list):
                                        for v in value:
                                            value_matrix[key].append(v)
                                    else:
                                        value_matrix[key].append(value)
                                else:
                                    if isinstance(value, list):
                                        value_matrix[key] = value
                                    else:
                                        value_matrix[key] = [value]
                        # print(f"{action._settings.action_name} value matrix finalized lengths: ", [len(vm) for _, vm in value_matrix.items()])
                        is_local = False
                        for key, val in value_matrix.items():
                            if required_sub_task.stored_database == host_name:
                                is_local = True
                                in_local_data_byte += asizeof.asizeof(val)
                            else:
                                is_local = False
                                in_remote_data_byte += asizeof.asizeof(val)
                            
                            datasets[key] = val
                        # print("value_matrix: ", value_matrix)
                        crr_tsd = TopicStatisticDetail()
                        crr_tsd.data_process_time = (time.perf_counter_ns() - current_in_topic_read_start_time) / 1000000.0
                        crr_tsd.data_size = asizeof.asizeof(value_matrix) / 1024.0
                        crr_tsd.local = is_local
                        
                        current_in_topic_records[required_sub_task.input_topic] = crr_tsd
                    else:
                        stat_lock.acquire()
                        statistic_record.miss_count += 1
                        stat_lock.release()
                        return
                    
                if not datasets:
                    stat_lock.acquire()
                    statistic_record.miss_count += 1
                    stat_lock.release()
                    return
                
                data = datasets
            
            event_publish_time = em_request.event_date
            data_read_time = time.perf_counter_ns() - current_time

            # execute the method
            # print("Executing the method...")
            task_execution_start_time = time.perf_counter_ns()
            in_data_byte = 0
            try:
                in_data_byte = asizeof.asizeof(data)
            except Exception as e:
                print("Exception while calculating input data size: ", str(e))
                return
            result = action.task(data)
            out_data_byte = 0
            try:
                out_data_byte = asizeof.asizeof(result)
            except Exception as e:
                print("Exception while calculating output data size: ", str(e))
                return
            
            process_ram_usage = psutil.virtual_memory().used / 1024.0
                
            task_execution_time = time.perf_counter_ns() - task_execution_start_time
            
            if not initDates or len(initDates) == 0:
                print("Initial data date cannot be null!!!!!!")
                print("Initial data date cannot be null!!!!!!")
                print("Initial data date cannot be null!!!!!!")
                print("Initial data date cannot be null!!!!!!")
                print("Initial data date cannot be null!!!!!!")
                print("Initial data date cannot be null!!!!!!")
                print("Initial data date cannot be null!!!!!!")
                raise Exception("Initial data date cannot be null!!!!!!")
            
            # save the data if required
            id = None
            output_topic:RequiredOutputTopics
            data_write_start_time = time.perf_counter_ns()
            to_db = {"data": pickle.dumps(result), "createdAt": datetime.utcnow(), "initDate": [min(initDates)]}
            
            if configs.DISABLE_VSM == 0:
                for output_topic in action._settings.output_topics:
                    if output_topic.to_database:
                        id = database_connection.insert_one(
                            to_db,
                            output_topic.output_topic,
                            output_topic.target_database,
                        )
            else:
                id = -1
                
            data_write_time = time.perf_counter_ns() - data_write_start_time

            # publish the result if required
            for output_topic in action._settings.output_topics:
                if action._settings.output_enabled:
                    if output_topic.target_database == host_name:
                        out_local_data_byte += out_data_byte
                    else:
                        out_remote_data_byte += out_data_byte
                    
                    # print("publishing result one")
                    # prepare the payload
                    em = EventModel()
                    em.data_id = id
                    if not output_topic.to_database:
                        em.data = result
                    em.event_date = datetime.utcnow()

                    if configs.DISABLE_VSM == 1:
                        em.data = to_db
                    else:
                        em.data = None

                    payload = pickle.dumps(em)

                    # publish
                    action_producer.produce(
                        output_topic.output_topic,
                        payload
                    )
                                
            total_elapsed = (datetime.utcnow() - event_publish_time).total_seconds()
            execution_elapsed = (time.perf_counter_ns() - current_time) / 1000000.0
            # if action._settings.action_name == "alarm_task":
            #     print("Elapsed time for alarm_task: ", datetime.utcnow(), " event publish time: ", event_publish_time, "  execution elapsed ms: ", execution_elapsed)
            
            ######################
            # CPU Calculations
            # https://stackoverflow.com/questions/41206809/psutil-measuring-the-cpu-usage-of-a-specific-process
            # https://github.com/giampaolo/psutil/issues/1989
            tid = threading.get_native_id()
            thread_cpu_times = [t for t in self.process.threads() if t.id == tid][0]
            thread_cpu_time = thread_cpu_times.user_time + thread_cpu_times.system_time
            
            # print(f"Thread id: {tid}, thread cpu time: {thread_cpu_time}")
            
            # Calculating how long it took to process each raw data if this is one of the last events
            raw_elapsed_time_from_init_ms = 0.0
            raw_elapsed_max_time_from_init_ms = 0.0
            raw_elapsed_min_time_from_init_ms = 0.0
            if action._settings.last_event:
                raw_elapsed_time_from_init_ms = 0.0
                raw_elapsed_max_time_from_init_ms = 0.0
                raw_elapsed_min_time_from_init_ms = 1000000
                in_d:datetime
                for in_d in initDates:
                    raw_elapsed_time_crr = (datetime.utcnow() - in_d).total_seconds()
                    raw_elapsed_time_from_init_ms += (raw_elapsed_time_crr * 1000.0)
                    
                    if raw_elapsed_time_crr > raw_elapsed_max_time_from_init_ms:
                        raw_elapsed_max_time_from_init_ms = raw_elapsed_time_crr
                    
                    if raw_elapsed_time_crr < raw_elapsed_min_time_from_init_ms:
                        raw_elapsed_min_time_from_init_ms = raw_elapsed_time_crr
                
                raw_elapsed_max_time_from_init_ms = raw_elapsed_max_time_from_init_ms * 1000.0
                raw_elapsed_min_time_from_init_ms = raw_elapsed_min_time_from_init_ms * 1000.0
                raw_elapsed_time_from_init_ms = raw_elapsed_time_from_init_ms / len(initDates)

            sst = SingleStatisticsRecord()
            sst.data_read_time = data_read_time / 1000000.0
            sst.data_write_time = data_write_time / 1000000.0
            sst.data_delete_time = data_delete_time / 1000000.0
            sst.total_elapsed = total_elapsed
            sst.task_execution_time = task_execution_time / 1000000.0
            sst.in_data_byte = in_data_byte / 1024.0
            sst.out_data_byte = out_data_byte  / 1024.0
            sst.execution_elapsed_ms = execution_elapsed
            sst.process_cpu_usage = thread_cpu_time
            sst.process_memory_usage = process_ram_usage
            sst.in_topic_records = current_in_topic_records
            sst.in_local_data_byte = in_local_data_byte / 1024.0
            sst.in_remote_data_byte = in_remote_data_byte / 1024.0
            sst.out_local_data_byte = out_local_data_byte / 1024.0
            sst.out_remote_data_byte = out_remote_data_byte / 1024.0
            sst.data_lifetime_status_type = data_lifetime_status_type
            sst.raw_elapsed_time_from_init_ms = raw_elapsed_time_from_init_ms
            sst.raw_elapsed_max_time_from_init_ms = raw_elapsed_max_time_from_init_ms
            sst.raw_elapsed_min_time_from_init_ms = raw_elapsed_min_time_from_init_ms
            
            stat_lock.acquire()
            # print(f"crr thread id: {tid}, action name: {action._settings.action_name}, topic name: {triggered_topic_name}, cpu_usage: {thread_cpu_time}")
            statistic_record.update_stats(sst, triggered_topic_name)
            statistic_record.update_topic_stats(data_lifetime_status_type, triggered_topic_name, True)
            statistic_record.hit_count += 1
            self.executed_task_count+=1
            self.processed_msg_count+=1
            stat_lock.release()
        except Exception as e:
            print("[ERROR] Probably action is deactivated before it can be finished: ", e)

    # an action goes through several steps to be registered
    # 1 - register to the requested incoming topic defined in mqtt_config as consumer
    # these run as background threads. confluent kafka is thread safe.
    # 2 - read the data from the event or from database depending on incoming event
    # 3 - execute the action and retrieve the data
    # 4 - store the data or publish it with event if required
    # 5 - remove the action on request of server for optimization goals
    # https://stackoverflow.com/questions/3061/calling-a-function-of-a-module-by-using-its-name-a-string
    # https://stackoverflow.com/questions/10773348/get-python-class-object-from-string
    def add_action(self, action: CEPTask):
        print(f"Adding action: {action._settings.action_name}, OS_TYPE: {configs.OS_TYPE}")
        
        # CPU calculations
        cpu_start_date = datetime.utcnow()
        if configs.OS_TYPE == 0:
            if configs.DEBUG_MODE:
                print("Preparing windows CPU counters")
            cpu_start_time = float(os.popen('cat /sys/fs/cgroup/cpuacct/cpuacct.usage').read())
            cpu_usage_times = os.popen('cat /sys/fs/cgroup/cpuacct/cpuacct.stat').readlines()
            cpu_start_usage_time_total = 0.0
            for cpu_usage_time in cpu_usage_times:
                cpu_start_usage_time_total += float(cpu_usage_time.split(" ")[1])
        elif configs.OS_TYPE == 1:
            if configs.DEBUG_MODE:
                print("Preparing linux CPU counters")
            cpu_usage_times = os.popen('cat /sys/fs/cgroup/cpu.stat').readlines()
            cpu_start_usage_time_total = 0.0
            counter = 0
            for cpu_usage_time in cpu_usage_times:
                if counter == 0:
                    cpu_start_time = float(cpu_usage_time.split(" ")[1])
                elif counter == 1 or counter == 2:
                    cpu_start_usage_time_total += float(cpu_usage_time.split(" ")[1])
                counter += 1
        
        if configs.DEBUG_MODE:
            print(f"cpu_start_date {cpu_start_date}")
            print(f"cpu_start_time {cpu_start_time}")
            print(f"cpu_usage_time_total {cpu_start_usage_time_total}")
        
        action.cpu_start_date = cpu_start_date
        action.cpu_start_hertz = cpu_start_time
        action.cpu_start_usage_hertz_total = cpu_start_usage_time_total
        
        add_action_start = time.perf_counter_ns()
        
        # download additional files
        if action._settings.additional_files:
            for additional_file in action._settings.additional_files:
                os.makedirs(os.path.dirname(additional_file), exist_ok=True)
                
                downloaded_additional_file = self.download_script_file(additional_file)
                
                print("Writing the file to: ", additional_file)
                
                with open(additional_file, "wb") as f:
                    f.write(downloaded_additional_file.content)

        #region activating the action
        # download the script from the server
        downloaded_module_file:requests.Response = self.download_script_file(action._settings.action_path)
        
        # save or overwrite the downloaded file, this assumes that at that specific point
        # the specific script is not being used.
        print("Writing the file to: ", action._settings.action_path)
        os.makedirs(os.path.dirname(action._settings.action_path), exist_ok=True)
        if configs.DEBUG_MODE:
            print("Downloaded the script file: ", action._settings.action_path)
        with open(action._settings.action_path+'.py', "wb") as f:
            f.write(downloaded_module_file.content)
        
        # importing the module where the requested action resides...
        # adds temporarily        
        splitted_path = action._settings.action_path.split('/')
        module_directory = '/'.join(splitted_path[0:len(splitted_path)-1])
        sys.path.append(module_directory)
        
        module_name = splitted_path[-1]
        module = importlib.import_module(module_name)
        
        imported_method = getattr(module, action._settings.action_name)
        # print (imported_method)
        sys.path.pop()
        
        if configs.DEBUG_MODE:
            print("[ACTION-ACTIVATION] Imported method is: ", imported_method)
        action.task = imported_method
        #endregion activating the action

        #region Create mongo collection and TTL index if not exists at the target database
        # This will wait until the target database can be reached
        # otherwise what is the point...
        output_topic:RequiredOutputTopics
        for output_topic in action._settings.output_topics:
            while True:
                try:
                    if configs.DEBUG_MODE:
                        print("Trying to make sure collection is created...")
                    db_succ = self.database_management_one.create_collection(
                        host_name = output_topic.target_database,
                        collection_name = output_topic.output_topic,
                        ttl_duration = configs.collection_lifetime
                    )
                    if db_succ:
                        break
                except Exception as e:
                    print(e)
                    pass
                # sleep 1 second per try
                time.sleep(1)
        if configs.DEBUG_MODE:
            print("Output topics are created...")
        #endregion Create mongo collection and TTL index if not exists at the target database

        # topics need to exists first and foremost
        # database should also be available otherwise no point in consuming
        # an event that we cannot process...
        required_sub_task:RequiredInputTopics=None
        
        # stats
        statistic_record:StatisticRecord
        if action._settings.action_name not in self.action_statistics:
            statistic_record = StatisticRecord(action._settings.action_name, 0)
            self.action_statistics[action._settings.action_name] = statistic_record
        else:
            statistic_record = self.action_statistics[action._settings.action_name]
            statistic_record.activation_time_ns = 0
            statistic_record.reset_stats()
            statistic_record.reset_cpu_stats()

        consumer_list = []
        consumer_threads = []
        producer_list = []
        producer_threads = []
        database_list = []
        input_topic_counter = InputTopicCounter()
        stat_lock = Lock()
        for required_sub_task in action._settings.required_sub_tasks:
            topic_name = required_sub_task.input_topic
            self.database_management_one.hold_till_connection_exists(required_sub_task.stored_database)
        
            df = DatabaseFactory()
            new_db_connection = df.get_database_service(groupid=f"{action._settings.action_name}_{required_sub_task.input_topic}")
            
            database_list.append(new_db_connection)
            
            # start the storage management
            Thread(daemon=True, target=new_db_connection.run).start() 

            new_db_connection.hold_till_connection_exists(required_sub_task.stored_database)
        
            # producer for publishing results
            action_producer = MQTTProducer(client_id=f"producer_{action._settings.action_name}_{topic_name}_{self._host_name}")        
        
            producer_list.append(action_producer)
        
            producer_threads.append(Thread(daemon=True, target=action_producer.run))
        
            # consumer for listening incoming events
            # setting groupid as action name makes it possible to consume an event only
            # once within the IoT cluster for this action
            action_consumer = MQTTConsumer(
                client_id=f"{action._settings.action_name}_{topic_name}_{self._host_name}",
                topic_name=[topic_name],
                target=self.execute_action,
                target_args=(action, topic_name, new_db_connection, stat_lock, statistic_record, self._host_name, action_producer, input_topic_counter)
            )
            consumer_list.append(action_consumer)
            
            # Keeping a counter for each input
            input_topic_counter.register_topic(topic_name)
            
            # Consumer thread
            consumer_threads.append(Thread(daemon=True, target=action_consumer.run))

        # save the action and its corresponding consumer and producer for future usage
        if configs.DEBUG_MODE:
            print("Current input topic counters: ", input_topic_counter.topics)
        self.actions[action._settings.action_name] = {
            "action": action,
            "consumer": consumer_list,
            "producer": producer_list,
            "databases": database_list,
            "module_name": module_name,
        }

        # for statistics
        add_action_end = (time.perf_counter_ns() - add_action_start) / 1000000.0
        statistic_record.activation_time_ns = add_action_end
            
        # Start consuming each topic
        for producer_thread in producer_threads:
            producer_thread.start()
        for consumer_thread in consumer_threads:
            consumer_thread.start()

        print("[*] worker activated action: ", action._settings.action_name, " ", imported_method)

    # https://stackoverflow.com/questions/32234156/how-to-unimport-a-python-module-which-is-already-imported
    def remove_action(self, action_name):
        if configs.DEBUG_MODE:
            print("---------------------------------------------")
            print("[!!] Removing action: ", action_name)
        action = self.actions[action_name]
        for consumer in action["consumer"]:
            consumer.close()
        for producer in action["producer"]:
            producer.close()
        for database in action["databases"]:
            database.close()
        self.action_statistics[action_name].reset_stats()
        self.action_statistics[action_name].reset_cpu_stats()
        
        if configs.DEBUG_MODE:
            print("Pre-validation of module existence: ", action["module_name"], flush=True)
        
        if action["module_name"] not in sys.modules:
            raise Exception("Module should exist here: ", action["module_name"])        
        
        # Unload the module
        del sys.modules[action["module_name"]]
        
        if configs.DEBUG_MODE:
            print("Deleted module: ", action["module_name"], flush=True)
                
        module_name = action["module_name"]
        
        del module_name
                                
        if action["module_name"] in sys.modules:
            raise Exception("Module still exists: ", action["module_name"])
        
        # is this name visible in the current scope:
        if action["module_name"] in dir():
            raise Exception("Module still exists in dir: ", action["module_name"])

        # or, is this a name in the globals of the current module:
        if action["module_name"] in globals():
            raise Exception("Module still exists in globals: ", action["module_name"])
        
        if configs.DEBUG_MODE:
            print("Validated module deletion: ", action["module_name"], flush=True)
        
        # Delete the downloaded file
        action_delete:CEPTask = action["action"]
        os.remove(action_delete._settings.action_path+'.py')
        
        del self.actions[action_name]
        if configs.DEBUG_MODE:
            print("[!] action %s removed." % (action_name))
            print("---------------------------------------------")

    # endregion managing the events

    def download_script_file(self, action_path)->requests.Response:
        server_url = 'http://'+configs.env_server_name+':'+str(configs.env_server_port)+'/get_server_action_file/'
        # print ('[*] server url is: ', server_url)
        try:
            params = {
                'file_path':action_path
                }
            return requests.get(server_url, params=params)
        except Exception as e:
            print ("[*] error during get request to server: ", e)
