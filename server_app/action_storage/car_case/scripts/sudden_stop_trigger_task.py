# does a redundant check if any of the incoming values are true
def sudden_stop_trigger_task(data):
    res = False
    if "slowdown" in data:
        # print("Slowdown length: ", len(data["slowdown"])) 
        for d in data["slowdown"]:
            res = res or (d == True)
    if "distance" in data:
        # print("distance length: ", len(data["distance"])) 
        for d in data["distance"]:
            res = res or (d < 30.0)
    if "object" in  data:
        # print("object length: ", len(data["object"])) 
        for d in data["object"]:
            res = res or (d == True)
    
    
    return {"stop": [res]}
