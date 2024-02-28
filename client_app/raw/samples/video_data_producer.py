from datetime import datetime
import os
import time

from fastapi import FastAPI
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer
import cv2
from pympler import asizeof
import client_configs

from raw.samples.sample_data_producers import SampleDataProducers

class VideoDataProducer(SampleDataProducers):
    def __init__(self, length, per_moment, per_moment_dist_type, sleep_duration, 
                 producer:RawDataProducer, app:FastAPI, settings:RawSettings, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration) -> None:
        print(f"[VideoDataProducer] Initializing with params: {per_moment}, {per_moment_dist_type}, {sleep_duration}")
        super().__init__(app, settings, per_moment_dist_type, length, per_moment, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration)
        self.producer = producer
        self.length = length
        self.per_moment = per_moment
        self.sleep_duration = sleep_duration
        
        #region Vid
        filename = os.environ['VIDEO_PATH']
        # clip = VideoFileClip(filename) 
        # frame = clip.get_frame(0)
        
        # read single frame
        clip = cv2.VideoCapture(filename)
        ret, frame = clip.read()
                
        # grayscaled_frame = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
        
        scale_percent = client_configs.RAW_IMAGE_SCALE_PERCENT # percent of original size
        width = int(frame.shape[1] * scale_percent / 100)
        height = int(frame.shape[0] * scale_percent / 100)
        dim = (width, height)
        
        # resize image
        self.resized = cv2.resize(frame, dim, interpolation = cv2.INTER_AREA)    
        self.resized = cv2.cvtColor(self.resized, cv2.COLOR_RGB2GRAY)
        
        #endregion Vid        
        print(f"[VideoDataProducer] Intialized")
        
        # generate which per moment value will be used during experiment
        self.per_moment_dist = self.get_next_moment_count()
        print(self.per_moment_dist)
        
    def run(self):    
        print(f"Activating the {self.settings.raw_data_name} data")

        print("Resized image shapes...")
        print(self.resized.shape)
        print(asizeof.asizeof(self.resized))
        
        # return
        
        staticvid_size = asizeof.asizeof(self.resized)
        staticvid_noise = [1] * int(staticvid_size/(8*4))
        
        print(f"Starting to send {self.settings.raw_data_name} sensor data...")
        
        msg_count=0
        # Do it N times
        for _, current_moment_length in enumerate(self.per_moment_dist):
            # Do this M times for this step
            for _ in range(current_moment_length):
                if self.killed: 
                    print(f"Stopping to send {self.settings.raw_data_name} sensor data...")
                    return True        
                
                # print("Sending frame number: ", msg_count)

                data = dict()
                data[self.settings.raw_data_name] = self.resized
                data["event_date"] = datetime.utcnow()
                                    
                self.producer.send(data)
                msg_count += 1
                crr_sleep_duration = (self.sleep_duration * 1.0) / current_moment_length
                # print(f"[WeightDataProducer] {datetime.utcnow()} sending data, sleep duration: {crr_sleep_duration}")
                time.sleep(crr_sleep_duration)
            
        print(f"Stopping to send {self.settings.raw_data_name} sensor data...")
        return msg_count
