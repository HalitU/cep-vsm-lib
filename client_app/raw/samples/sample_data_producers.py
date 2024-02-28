from copy import deepcopy
import math
from fastapi import FastAPI, Response
from cep_library.raw.model.raw_settings import RawSettings
import numpy as np
import random

class SampleDataProducers:
    def __init__(self, app:FastAPI, settings:RawSettings, per_moment_dist_type, length, per_moment, raw_output_window_min_threshold, raw_output_window_max_threshold, raw_output_window_random_seed,raw_output_window_duration, raw_output_size_min=None, raw_output_size_max=None) -> None:
        print(f"[SampleDataProducers] Initializing with params: {per_moment}, {per_moment_dist_type}")
        self.killed = False
        self.per_moment_dist_type = per_moment_dist_type
        self.length = length
        self.per_moment = per_moment
        self.settings = settings
        
        self.raw_output_window_min_threshold = raw_output_window_min_threshold
        self.raw_output_window_max_threshold = raw_output_window_max_threshold
        self.raw_output_window_random_seed = raw_output_window_random_seed
        self.raw_output_window_duration = raw_output_window_duration
        
        self.raw_output_size_min = raw_output_size_min
        self.raw_output_size_max = raw_output_size_max
        
        @app.get("/stop_sampling/"+settings.raw_data_name+"/")
        def stop_sampling():
            self.stop()
            return Response(status_code=200)
        
        @app.get("/start_sampling/"+settings.raw_data_name+"/")
        def start_sampling():
            self.killed = False
            self.run()
            return Response(status_code=200)        
                
                
        @app.get("/test_sampling/"+settings.raw_data_name+"/")
        def start_sampling(required_len:int):
            old_dist_len = self.per_moment_dist
            self.per_moment_dist = [required_len]
            self.killed = False
            self.run()
            self.per_moment_dist = old_dist_len
            return Response(status_code=200)                
                                
    def stop(self):
        self.killed = True
    
    # A very basic approach to have different raw data sizes in different halves
    def get_per_moment_size_dist(self):
        per_moment_sizes = []
        
        for _ in range(math.ceil(self.length/2)):
            per_moment_sizes.append(self.raw_output_size_min)
            
        for _ in range(math.ceil(self.length/2)):
            per_moment_sizes.append(self.raw_output_size_max)
        
        return per_moment_sizes
    
    def get_next_moment_count(self)->[]:
        # static
        if self.per_moment_dist_type == 1:
            print("Utilizing static distribution")
            return [self.per_moment] * self.length
        
        # uniform dist
        if self.per_moment_dist_type == 2:
            print("Utilizing uniform distribution")
            return np.random.randint(1, self.per_moment, self.length)
        
        # exponential/aka increasing load
        if self.per_moment_dist_type == 3:
            print("Utilizing exponential start distribution")
            return np.logspace(1,self.per_moment,num=self.length,base=1.26,dtype='int')
        
        # reverse exponential
        if self.per_moment_dist_type == 4:
            print("Utilizing exponential end distribution")
            dist = np.logspace(1,self.per_moment,num=self.length,base=1.26,dtype='int')
            return np.flip(dist)
            # list.reverse(dist)
            # return dist

        # kinda normal distribution
        if self.per_moment_dist_type == 5:
            print("Utilizing pyramid distribution")
            flt = self.per_moment * 1.0 / (self.length / 2.0)
            
            res = []
            crr = flt
            for i in range(math.floor(self.length/2.0)):
                res.append(math.ceil(crr))
                crr += flt

            res_org = deepcopy(res)
            res.reverse()
            res_org = res_org + res
                
            return res_org            

        # fluctuating window distribution
        if self.per_moment_dist_type == 6:
            print("Utilizing fluctuating distribution...")
            # ex: 1800 / 30 = 60 windows
            window_count = self.length / self.raw_output_window_duration
            # should be equal to length
            per_second_counts = []
            # setting seed so the distribution wont change between simulations for fairness
            random.seed(self.raw_output_window_random_seed)
            # for each windows ex: 60
            for _ in range(math.ceil(window_count)):
                # get a count between min and max threshold: ex: 150
                crr_count = random.randint(self.raw_output_window_min_threshold, self.raw_output_window_max_threshold)
                
                # each seconds this much will be produced ex: 150 / 30 = 5
                per_second_count = math.ceil(crr_count / self.raw_output_window_duration)
                for _ in range(self.raw_output_window_duration):
                    # for this window duration add this count ex: 5 for each second in 30
                    per_second_counts.append(per_second_count)

            # this count should be equal to length
            if len(per_second_counts) != self.length:
                raise Exception("Fault occured during generating fluctuating data!")
            
            return per_second_counts

        # First half high ceiling
        if self.per_moment_dist_type == 7:
            per_second_counts = []
            
            for _ in range(math.ceil(self.length/2)):
                per_second_counts.append(self.raw_output_window_max_threshold)
                
            for _ in range(math.ceil(self.length/2)):
                per_second_counts.append(self.raw_output_window_min_threshold)
            
            return per_second_counts
        
        # Second half high ceiling
        if self.per_moment_dist_type == 8:
            per_second_counts = []
            
            for _ in range(math.ceil(self.length/2)):
                per_second_counts.append(self.raw_output_window_min_threshold)
                
            for _ in range(math.ceil(self.length/2)):
                per_second_counts.append(self.raw_output_window_max_threshold)            
            
            return per_second_counts
