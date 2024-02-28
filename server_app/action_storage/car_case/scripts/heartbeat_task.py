import numpy as np

# Iterates through several health values and returns one of the important ones
def heartbeat_task(data):    
    if "beat" in data:
        destination = 0.0
        min_d = 5
        max_d = 100        
        for d in data["beat"]:
            roadby_data = {
                'dat1': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat2': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat3': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat4':(max_d - min_d) * np.random.random_sample() + min_d, 
                'dat5': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat6': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat7':(max_d - min_d) * np.random.random_sample() + min_d, 
                'dat8': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat9': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat10':(max_d - min_d) * np.random.random_sample() + min_d, 
                'dat11': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat12': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat13':(max_d - min_d) * np.random.random_sample() + min_d, 
                'dat14': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat15': (max_d - min_d) * np.random.random_sample() + min_d, 
                'dat16':(max_d - min_d) * np.random.random_sample() + min_d
                }
            destination = roadby_data['dat12']
        return {"health": [destination]}
    else:
        return {"health": [0.0]}
