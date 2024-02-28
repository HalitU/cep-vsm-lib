import time

from paho.mqtt import client as mqtt_client

from cep_library import configs


class MQTTProducer:
    def __init__(self, client_id) -> None:        
        self.client_id = client_id
        
        if configs.DEBUG_MODE:
            print(f"Producer connecting to {configs.MQTT_HOST}, {configs.MQTT_PORT}, with client_id: {client_id}")
        self.client:mqtt_client.Client = mqtt_client.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        
        while True:
            try:
                self.client.connect(configs.MQTT_HOST, configs.MQTT_PORT, 60)
                break
            except Exception as e:
                print("Waiting to connect to MQTT broker...")
                pass
            time.sleep(1.0)
        if configs.DEBUG_MODE:
            print("[MQTTProducer] created.")
    
    def on_connect(self, client:mqtt_client.Client, userdata, flags, rc):
        if configs.DEBUG_MODE:
            print("Connected with result code "+str(rc))    
        
    def on_disconnect(self, client, userdata, rc):
        if configs.DEBUG_MODE:
            print(f"{self.client_id} Client disconnected...")            
        
    def close(self):
        self.client.disconnect()        
        
    def produce(self, topic, msg, on_delivery=None):
        self.client.publish(topic=topic, payload=msg, qos=configs.MQTT_QOS_LEVEL)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.        
    def run(self):
        if configs.DEBUG_MODE:
            print("Running the blocking loop...")
        try:
            self.client.loop_forever()
        except Exception as e:
            print("Unexpected error occured during loop: ", str(e))
        if configs.DEBUG_MODE:
            print("Timeout or disconnection happened...")
