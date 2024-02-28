import time

from paho.mqtt import client as mqtt_client

from cep_library import configs


class MQTTConsumer:
    def __init__(self, client_id, topic_name, target, target_args=None, qos:int=0) -> None:
        self.topic_name = topic_name[0]
        self.target = target
        self.target_args = target_args
        self.qos = qos
        self.client_id = client_id
        
        if configs.DEBUG_MODE:
            print(f"Consumer connecting to {configs.MQTT_HOST}, {configs.MQTT_PORT}, with client id: {self.client_id}")
        self.client:mqtt_client.Client = mqtt_client.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
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
            print("[MQTTConsumer] created.")
    
    def on_connect(self, client:mqtt_client.Client, userdata, flags, rc):
        if configs.DEBUG_MODE:
            print("Connected with result code "+str(rc))      
    
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(self.topic_name, qos=configs.MQTT_QOS_LEVEL)
        
    def on_disconnect(self, client, userdata, rc):
        if configs.DEBUG_MODE:
            print(f"{self.client_id} Client disconnected...")      
        
    def on_subscribe(self, mosq, obj, mid, granted_qos):
        if configs.DEBUG_MODE:
            print("Subscribed: " + str(mid) + " " + str(granted_qos))        
        
    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.target(msg.payload, self.target_args)
    
    # https://stackoverflow.com/questions/35580906/paho-mqtt-client-connection-reliability-reconnect-on-disconnection
    # https://stackoverflow.com/questions/49032927/how-to-exit-mqtt-forever-loop
    def close(self):
        self.client.disconnect()
        
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
