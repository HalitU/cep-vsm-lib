import math
import time

from cep_library import configs
import client_configs

class RequiredRawManipulator:
    def __init__(self, length, low_threshold, high_threshold, window_length, window_dist_type:int=0) -> None:
        self.window_length = window_length
        self.length = length
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        self.window_dist_type = window_dist_type
        self.dist = self.get_next_moment_count()
        print("Manipulator type: ", self.window_dist_type)
        print("Setting up the manipulator with array: ", self.dist)

    def run(self):
        if client_configs.RAW_REQUIRED_AMOUNT_DIST_ACTIVE == 1:
            print("Starting the required manipulator")
            for d in self.dist:
                configs.MIN_REQUIRED_ACTIVE_VAR = d
                time.sleep(1.0)

    def get_next_moment_count(self)->[]:
        per_second_counts = []
        
        if self.window_dist_type == 0:
            for _ in range(math.ceil(self.length/self.window_length)):
                per_second_counts.append(self.low_threshold)
                
            for _ in range(math.ceil(self.length/self.window_length)):
                per_second_counts.append(self.high_threshold)
        
        elif self.window_dist_type == 1:
            window_size = math.ceil(self.length / self.window_length)
            fluctuate = 0
            
            # For each window, fluctuate the requirement e.g.: 10
            for _ in range(self.window_length):
                # in the window duration create the requirement e.g.: 1800/10 = 180
                for _ in range(window_size):
                    if fluctuate == 0:
                        per_second_counts.append(self.low_threshold)
                    elif fluctuate == 1:
                        per_second_counts.append(self.high_threshold)
                    elif fluctuate == 2:
                        per_second_counts.append(math.ceil((self.low_threshold + self.high_threshold) / 2.0))
                    else:
                        raise Exception("Invalid fluctutation type")
                        
                # switch to next type
                if fluctuate == 0:
                    fluctuate = 1
                elif fluctuate == 1:
                    fluctuate = 2
                elif fluctuate == 2:
                    fluctuate = 0
        
        return per_second_counts
