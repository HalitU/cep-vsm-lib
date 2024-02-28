import numpy as np
import math
        
def maintenance_guesser_task(data):    
    if "road_data" in data:
        min_d = 5
        max_d = 100
        dist = False
        for d in data["road_data"]:
            x1 = (max_d - min_d) * np.random.random_sample() + min_d
            x2 = (max_d - min_d) * np.random.random_sample() + min_d
            
            y1 = (max_d - min_d) * np.random.random_sample() + min_d
            y2 = (max_d - min_d) * np.random.random_sample() + min_d
            
            dist = math.dist([x1, x2], [y1, y2])
            
            x1 = (max_d - min_d) * np.random.random_sample() + min_d
            x2 = (max_d - min_d) * np.random.random_sample() + min_d
            
            y1 = (max_d - min_d) * np.random.random_sample() + min_d
            y2 = (max_d - min_d) * np.random.random_sample() + min_d
            
            dist = math.dist([x1, x2], [y1, y2])            
            
            x1 = (max_d - min_d) * np.random.random_sample() + min_d
            x2 = (max_d - min_d) * np.random.random_sample() + min_d
            
            y1 = (max_d - min_d) * np.random.random_sample() + min_d
            y2 = (max_d - min_d) * np.random.random_sample() + min_d
            
            dist = math.dist([x1, x2], [y1, y2])            
            
            if dist < ((min_d + max_d) * 1.0 / 2):
                dist = dist or False
            else:
                dist = dist or True
        return {"maintenance": [dist]}
    else:
        return {"maintenance": [False]}
