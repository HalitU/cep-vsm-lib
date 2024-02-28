from datetime import datetime
import time

from fastapi import FastAPI
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer
import numpy as np

from raw.samples.sample_data_producers import SampleDataProducers

class WeightDataProducer(SampleDataProducers):
    def __init__(self, length, per_moment, per_moment_dist_type, sleep_duration, producer:RawDataProducer, app:FastAPI, settings:RawSettings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration) -> None:
        print(f"[WeightDataProducer] Initializing with params: {per_moment}, {per_moment_dist_type}, {sleep_duration}")
        super().__init__(app, settings, per_moment_dist_type, length, per_moment, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration)
        self.producer = producer
        self.length = length
        self.per_moment = per_moment
        self.sleep_duration = sleep_duration
        
        # generate which per moment value will be used during experiment
        self.per_moment_dist = self.get_next_moment_count()
        print("[WeightDataProducer] Weight distribution average: ", np.average(self.per_moment_dist))
        print(self.per_moment_dist)
        
    def run(self):    
        print(f"[CLIENT] Starting to send {self.settings.raw_data_name} sensor data...")
        msg_count = 0
        weight_min = 0.0 
        weight_max = 120.0
                
        # Do it N times
        for _, current_moment_length in enumerate(self.per_moment_dist):
            # Do this M times for this step
            for _ in range(current_moment_length):
                if self.killed: 
                    print(f"[CLIENT] Stopping to send {self.settings.raw_data_name} sensor data...")
                    return True        
                
                data = {}
                data[self.settings.raw_data_name] = (weight_max - weight_min) * np.random.random_sample() + weight_min
                data["event_date"] = datetime.utcnow()
                    
                self.producer.send(data)
                
                msg_count += 1
                crr_sleep_duration = (self.sleep_duration * 1.0) / current_moment_length
                # print(f"[WeightDataProducer] {datetime.utcnow()} sending data, sleep duration: {crr_sleep_duration}")
                time.sleep(crr_sleep_duration)
            
        print(f"[CLIENT] Stopping to send {self.settings.raw_data_name} sensor data...")
            
        return msg_count