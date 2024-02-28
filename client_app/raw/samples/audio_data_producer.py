from datetime import datetime
import os
import time

from fastapi import FastAPI
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer
from pympler import asizeof
import librosa

from raw.samples.sample_data_producers import SampleDataProducers

class AudioDataProducer(SampleDataProducers):
    def __init__(self, length, per_moment, per_moment_dist_type, sleep_duration, producer:RawDataProducer, app:FastAPI, settings:RawSettings) -> None:
        print(f"[AudioDataProducer] Initializing with params: {per_moment}, {per_moment_dist_type}, {sleep_duration}")
        super().__init__(app, settings, per_moment_dist_type, length, per_moment)
        self.producer = producer
        self.length = length
        self.per_moment = per_moment
        self.sleep_duration = sleep_duration
        self.per_moment_dist_type = per_moment_dist_type
        
        sound_file = os.environ['AUDIO_PATH']
        self.audio_sample, self.sampling_rate = self.process_sample_audio_file(sound_file, 0.05, 0.05)

        print(f"[AudioDataProducer] Initialized")
        
        # generate which per moment value will be used during experiment
        self.per_moment_dist = self.get_next_moment_count()
        print(self.per_moment_dist)

    def process_sample_audio_file(self, filename, offset, duration):
        y, sr = librosa.load(filename, offset=offset, duration=duration, mono=False, sr=None)
        return y, sr

    """Duration of 0.05 seconds is around 4648 bytes
    """    
    def run(self):
        audio_length:float=0.05
        duration:float=0.05
        
        print(f"Activating the {self.settings.raw_data_name} data")

        staticdata_size = asizeof.asizeof(self.audio_sample)
        staticnoise = [1] * int(staticdata_size/(8*4))
        
        print(f"Starting to send {self.settings.raw_data_name} sensor data...")
        
        msg_count = 0
        # Do it N times
        for _, current_moment_length in enumerate(self.per_moment_dist):
            # Do this M times for this step
            for _ in range(current_moment_length):
                # Do until offset hits the last moment of the audio file
                offset = 0.0
                while offset < audio_length:   
                    if self.killed: 
                        print(f"Stopping the {self.settings.raw_data_name} data")
                        return True        
                    
                    data = {}
                    data[self.settings.raw_data_name] = self.audio_sample
                    data["sampling_rate"] = self.sampling_rate
                    data["event_date"] = datetime.utcnow()
                    
                    self.producer.send(data)
                    
                    msg_count += 1
                    offset += duration
                # Sleep 1 second and start the next step
                crr_sleep_duration = (self.sleep_duration * 1.0) / current_moment_length
                # print(f"[WeightDataProducer] {datetime.utcnow()} sending data, sleep duration: {crr_sleep_duration}")
                time.sleep(crr_sleep_duration)
            
        print(f"Stopping the {self.settings.raw_data_name} data")
        return msg_count
