from dotenv import load_dotenv
from os import environ
from pytube import YouTube
from paho.mqtt import client as mqtt_client
import random
import topics
import logging

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT")
    else:
        logging.fatal("Failed to connect to MQTT, return code: %d\n", rc)

def on_message(client, userdata, msg):
    url = msg.payload.decode()
    YouTube(url).streams.first().download()

def setup_mqtt(broker: str, port: int = 1883):
    client_id = "python-mqtt-{}".format(random.randint(0, 1000))

    client = mqtt_client.Client(client_id=client_id)
    client.on_connect = on_connect
    client.connect(broker, port=port)

    client.subscribe(topic=topics.VIDEO_DOWNLOAD)
    client.on_message = on_message
    client.loop_forever()

def main():
    load_dotenv()
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    broker_host = environ.get("MQTT_HOST")
    if broker_host is None:
        logging.fatal("broker host is empty")
    broker_port = environ.get("MQTT_PORT")
    if broker_port is None:
        broker_port = 1883
    else:
        broker_port = int(broker_port)

if __name__ == '__main__':
    main()