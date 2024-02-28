import pickle
import time
from datetime import datetime, timedelta
from threading import Lock, Thread

import pymongo
import requests
from fastapi import FastAPI, Response
from pympler import asizeof

import cep_library.configs as configs
from cep_library.cep.model.cep_task import EventModel, MongoQuery, RequiredOutputTopics
from cep_library.data.database_management_service import BaseDatabaseManagement
from cep_library.data.model.database_info_model import DataCarryModel
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer
from cep_library.raw.model.raw_settings import RawSettings, RawStatistics, RawStatisticsBook
from cep_library.raw.model.raw_update_event import RawUpdateEvent


class RawDataProducer:
    def __init__(self, settings:RawSettings, db:BaseDatabaseManagement, app: FastAPI) -> None:
        # database connection
        self.database_management_one = db
        for output_topic in settings.output_topics:
            self.database_management_one.create_collection(settings.producer_name, output_topic.output_topic, 
                                                           configs.collection_lifetime)
        
        # keeping the settings of the producer class
        self.settings:RawSettings = settings
        self.killed = False
        self.db_lock = Lock()
        self.produced_id_ctr = 0
        
        # register to the server, retry until success
        self.send_heartbeat()
            
        # create the kafka producer
        self.message_producer = MQTTProducer(
            client_id=f"producer_{configs.kafka_producer_topics[0]}_{settings.raw_data_name}")
        
        # create the kafka consumer for target database updates
        self.message_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_producer_topics[0]}_{settings.raw_data_name}",
            topic_name=configs.kafka_producer_topics,
            target=self.update_data_target,
            target_args=None,
        )
        
        self.raw_statistics = RawStatisticsBook(
            producer_name = settings.producer_name,
            raw_name = settings.raw_data_name,
            record_count=0,
            write_time_ns_total = 0.0,
            write_size_byte_total = 0.0
        )
        self.register_action_file_service(app)
        
        Thread(daemon=True, target=self.message_producer.run).start()
        Thread(daemon=True, target=self.message_consumer.run).start()
        
        # start the storage management
        # Thread(daemon=True, target=self.database_management_one.run).start()
        
        # Start sending periodic heartbeats to the server
        Thread(daemon=True, target=self.heartbeat).start()

    def get_settings(self):
        return self.settings

    def register_action_file_service(self, app: FastAPI):
        @app.get("/raw_statistics_report/"+self.settings.raw_data_name+"/")
        def raw_statistics_report():    
            usage = self.raw_statistics
            encoded = pickle.dumps(usage)
            
            # check if locking is required!
            self.raw_statistics.reset_stats()
            
            return Response(content=encoded, media_type="application/json")        
        
    def send(self, data):       
        self.db_lock.acquire()
                
        converted_data = {'data': pickle.dumps(data), "createdAt": datetime.utcnow(), "initDate": [datetime.utcnow()]}
                
        start_time = time.perf_counter_ns()
                
        output_topic:RequiredOutputTopics=None
        for output_topic in self.settings.output_topics:     
            # print (f"printing to output topic: {output_topic}, and to database, {output_topic.target_database}")
            # Save it to the target databases
            if configs.DISABLE_VSM == 1:
                id = self.produced_id_ctr
            else:
                id = self.database_management_one.insert_one(
                    converted_data,
                    output_topic.output_topic,
                    output_topic.target_database,
                )
            
            # Notify the required events
            em = EventModel()
            em.data_id = id
            em.event_date = datetime.utcnow()

            if configs.DISABLE_VSM == 1:
                em.data = converted_data
            else:
                em.data = None

            payload = pickle.dumps(em)

            # print("publishing result: ", payload)
            # publish
            self.message_producer.produce(
                output_topic.output_topic,
                payload,
                on_delivery=self.ack,
            )  
            
            self.produced_id_ctr += 1
            
        end_time = (time.perf_counter_ns() - start_time) * 1.0 / 1000000.0
        data_size = asizeof.asizeof(converted_data) * 1.0 / 1024.0
            
        stat = RawStatistics(
            write_time_ns = end_time,
            write_size_byte = data_size
        )
        self.raw_statistics.add_raw_statistic(stat)
        
        self.db_lock.release()
            
    def ack(self, err, msg):
        pass   
    
    def heartbeat(self):
        while not self.killed:
            self.send_heartbeat()
            time.sleep(1.0)
    
    def send_heartbeat(self):
        # wait until server sends ready response
        ready = False or configs.env_server_name == 'localhost'
        server_url = 'http://'+configs.env_server_name+':'+str(configs.env_server_port)+'/raw_producer/'
        
        while not ready:
            try:
                setting_bytes = pickle.dumps(self.settings)
                ready = requests.post(server_url, data=setting_bytes)
            except Exception as e:
                print ("[*] error during get request to server: ", e)
            # retry after 1 second
            time.sleep(1)
    
    def stop(self):
        self.message_producer.close()
        self.message_consumer.close()
        self.database_management_one.close()
        self.killed = True
        
    def update_data_target(self, msg: bytes, args): 
        target_update_request: RawUpdateEvent = pickle.loads(msg)
                
        if self.settings.producer_name != target_update_request.producer_name: return
                            
        # Check if any of the output topic targets require updating
        output_topic:RequiredOutputTopics
        for output_topic in target_update_request.output_topics:
            current_output_topic = [a for a in self.settings.output_topics if a.output_topic == output_topic.output_topic]
            if current_output_topic:
                # print (f"Current output topic: {current_output_topic}")
                # print (f"Current output topic to check: {current_output_topic[0]}, target database {output_topic.target_database}")
                
                current_output_topic = current_output_topic[0]
                if current_output_topic.target_database == output_topic.target_database:
                    # print(f"Target database is already {output_topic.target_database}")
                    continue
                else:
                    print("[P] Target database update request is valid, starting the alteration...")
                    
                    # wait until device is connected to the other database
                    self.database_management_one.hold_till_connection_exists(output_topic.target_database)
                    
                    self.db_lock.acquire() 
                    
                    # create collection with index if it does not exist in target database!
                    self.database_management_one.create_collection(output_topic.target_database, output_topic.output_topic, configs.collection_lifetime)                    
                    
                    # update settings
                    old_database = current_output_topic.target_database
                    current_output_topic.target_database = output_topic.target_database
                    
                    # carry over last T data from the old db to new one if required
                    if configs.data_carry_enabled:
                        query = MongoQuery()
                        query.sort = [("createdAt", pymongo.DESCENDING)]
                        query.within_timedelta = timedelta(seconds=configs.data_carry_window)
                        query.columns = { "_id": 1, "data": 1, "initDate": 1 }
                        
                        data_carry_model = DataCarryModel()
                        data_carry_model.source_host = old_database
                        data_carry_model.target_host = output_topic.target_database
                        data_carry_model.collection = output_topic.output_topic
                        data_carry_model.remove_source = True
                        data_carry_model.query = query
                        
                        self.database_management_one.carry_data(data_carry_model)
                    
                    self.db_lock.release() 
                    
                    print("[P] Target database update completed")
        