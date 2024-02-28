import numpy as np

# gets several distance data from a roadby sensor
# which gives current distance to important places  
def destination_calculator_task(data):    
    if "road_data" in data:
        min_d = 5
        max_d = 100        
        destination = 0.0
        for d in data["road_data"]:
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
            
        return {"destination": [destination]}
    else:
        return {"destination": [0.0]}
