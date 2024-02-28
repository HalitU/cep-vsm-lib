import numpy as np

def nearby_road_task(data):
    if "wifi" in data:
        destination = 0.0
        min_d = 5
        max_d = 100
        for d in data["wifi"]:
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
                'dat13':(max_d - min_d) * np.random.random_sample() + min_d
                }
            destination = roadby_data['dat9']
        return {"road_data": [destination]}
    else:
        return {"road_data": [0.0]}
