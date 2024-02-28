import cv2
import numpy as np

face_cascade = cv2.CascadeClassifier('sample_data/haarcascade_smile.xml')

def coinFlip(p):
    return np.random.binomial(1, p)

# does redundant bool checks and applies a redundant cascade object detection classifier
def object_detection_task(data):
    res = False
    if "distance" in data:
        for d in data["distance"]:
            res = res or (d < 30.0)
        
    if "image" in data:
        for im in data["image"]:
            face_cascade.detectMultiScale(im)
            res = res or (coinFlip(0.5) == 1)
            
    if "health" in data:
        for d in data["health"]:
            res = res or (d > 70.0)
    
    return {"object": [res]}
