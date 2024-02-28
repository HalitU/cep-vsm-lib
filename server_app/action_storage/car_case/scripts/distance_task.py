import numpy as np
import math
    
# calculates several distances to nearby objects
def distance_task(data):
    if "distance" in data:
        min_d = 5
        max_d = 100
        dist = 0.0
        for d in data["distance"]:
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
            
        return {"distance": [dist]}
    else:
        return {"distance": [0.0]}
