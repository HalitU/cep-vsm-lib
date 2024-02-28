from raw.samples.temperature_data_producer import TemperatureDataProducer
from fastapi import FastAPI
from cep_library.data.database_management_service import BaseDatabaseManagement
from cep_library.raw.model.raw_settings import OutputTopics, RawSettings
from cep_library import configs
from cep_library.raw.raw_data_producer import RawDataProducer
from raw.samples.video_data_producer import VideoDataProducer
from raw.samples.weight_data_producer import WeightDataProducer

class RawProducerFactory:
    def __init__(self, db:BaseDatabaseManagement, app:FastAPI) -> None:
        self.db = db
        self.app = app
        print("[RawProducerFactory] Initialized")
    
    def get_sensor(self, output_topic_name:str, raw_data_name:str, expected_cost:int):
        out_topics = OutputTopics(output_topic=output_topic_name, target_database=configs.env_host_name)
        
        temperature_raw_settings = RawSettings(
            raw_data_name = raw_data_name,
            producer_name = configs.env_host_name,
            producer_port = configs.env_host_port,
            db_host = configs.env_mongo_host,
            db_port = configs.env_mongo_port,
            consumer_group = f"producer_{configs.env_host_name}_{out_topics.output_topic}",
            output_topics = [out_topics],
            expected_cost = expected_cost
        )
        
        return RawDataProducer(temperature_raw_settings, db=self.db, app=self.app)

    def get_sensor_producer(self, raw_type, length, per_moment, per_moment_dist_type, sleep_duration, producer, app, settings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration, raw_output_size_min, raw_output_size_max):
        print(f"[RawProducerFactory] Getting a sensor with params: {length}, {per_moment}, {per_moment_dist_type}, {sleep_duration}")
        # temp
        if raw_type == 1:
            return TemperatureDataProducer(length, per_moment, per_moment_dist_type, sleep_duration, producer, app, settings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration, raw_output_size_min, raw_output_size_max)

        # video
        if raw_type == 3:
            return VideoDataProducer(length, per_moment, per_moment_dist_type, sleep_duration, producer, app, settings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration)
            
        # weight
        if raw_type == 4:
            return WeightDataProducer(length, per_moment, per_moment_dist_type, sleep_duration, producer, app, settings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration)
        
