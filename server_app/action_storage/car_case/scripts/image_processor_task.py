import cv2
        
def expensive_video_task(frame):
    return cv2.GaussianBlur(frame,(5,5),cv2.BORDER_DEFAULT)

# does a gaussian blue filter on incoming frames and returns one of them
def image_processor_task(data):        
    if "frame" in data:
        res_f = None
        for f in data["frame"]:
            # resized_image = resize_image(f)
            # grayscaled_frame = cv2.cvtColor(resized_image, cv2.COLOR_RGB2GRAY)
            filtered_image = expensive_video_task(f)
            res_f = filtered_image
        return {"image": [res_f]}
    else:
        return {"image": []}
