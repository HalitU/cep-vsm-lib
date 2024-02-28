# does a redundant check if any of the incoming values are true
def gas_alarm_predictor_task(data):            
    res = False
    if "destination" in data:
        for d in data["destination"]:
            res = res or (d > 50.0)
        
    if "maintenance" in data:
        for d in data["maintenance"]:
            res = res or (d)
    
    return {"gas": [res]}
