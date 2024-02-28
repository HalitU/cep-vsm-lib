from datetime import datetime
import time

from fastapi import FastAPI
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer
import numpy as np

from raw.samples.sample_data_producers import SampleDataProducers

class TemperatureDataProducer(SampleDataProducers):
    def __init__(self, length, per_moment, per_moment_dist_type, sleep_duration, producer:RawDataProducer, app:FastAPI, settings:RawSettings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration, raw_output_size_min, raw_output_size_max) -> None:
        print(f"[TemperatureDataProducer] Initializing with params: {per_moment}, {per_moment_dist_type}, {sleep_duration}")
        super().__init__(app, settings, per_moment_dist_type, length, per_moment, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration, raw_output_size_min, raw_output_size_max)
        self.producer = producer
        self.length = length
        self.per_moment = per_moment
        self.sleep_duration = sleep_duration
        
        # generate which per moment value will be used during experiment
        self.per_moment_dist = self.get_next_moment_count()
        self.per_moment_size_dist = self.get_per_moment_size_dist()
        print(self.per_moment_dist)
        print("[TemperatureDataProducer] Time distribution averag: ", np.average(self.per_moment_dist))
        
    def run(self):    
        print(f"Starting {self.settings.raw_data_name} sampling")
        msg_count = 0
        temp_min = -10.0 
        temp_max = 40.0
        
        # Do it N times
        for _, (current_moment_length, current_moment_size) in enumerate(zip(self.per_moment_dist, self.per_moment_size_dist)):
            # Do this M times for this step
            for _ in range(current_moment_length):
                if self.killed: 
                    print(f"Stopping {self.settings.raw_data_name} sampling") 
                    return True        
                
                data = dict()
                data[self.settings.raw_data_name] = [(temp_min - temp_max) * np.random.random_sample() + temp_max] * current_moment_size
                data["event_date"] = datetime.utcnow()

                self.producer.send(data)
                
                msg_count += 1
                crr_sleep_duration = (self.sleep_duration * 1.0) / current_moment_length
                # print(f"[TemperatureDataProducer] {datetime.utcnow()} sending data, sleep duration: {crr_sleep_duration}")
                time.sleep(crr_sleep_duration)
            
        print(f"Stopping {self.settings.raw_data_name} sampling")
        return msg_count
