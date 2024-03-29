from dotenv import load_dotenv
from os import environ
from pytube import YouTube
from paho.mqtt import client as mqtt_client
import random
import topics
import logging
import datetime
import uuid

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT")
    else:
        logging.fatal("Failed to connect to MQTT, return code: %d\n", rc)

def on_message(client: mqtt_client.Client, userdata, msg):
    url = msg.payload.decode()
    logging.info(f"got url to dowanload: {url}")
    try:
        yt = YouTube(url)
        output_file_name_uuid = str(uuid.uuid4())
        logging.info(f"downloading video title: {yt.title}, generated uuid: {output_file_name_uuid}")
        yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first().download(filename=f'{output_file_name_uuid}.mp4', output_path='videos')
    except Exception as ex:
        client.publish(topic=topics.SEND_MESSAGE, payload="Failed to start video download")
        if hasattr(ex, 'message'):
            logging.error(ex.message)
        else:
            logging.error(ex)
        return
    logging.info(f"finished to download video: {url}")
    res = client.publish(topic=topics.SEND_MESSAGE, payload=f"Video download finished: {yt.title}. Id: {output_file_name_uuid}")
    if res.rc == 0:
        logging.info("message sent to {}".format(topics.SEND_MESSAGE))
    else:
        logging.fatal("failed to connect to MQTT, code: %d\n", res)


def setup_mqtt(broker: str, port: int = 1883, broker_username: str | None = None, broker_password: str | None = None):
    client_id = "yt-mqtt-{}".format(random.randint(0, 1000))

    client = mqtt_client.Client(client_id=client_id)
    client.on_connect = on_connect
    if broker_username is not None:
        client.username_pw_set(broker_username, broker_password)
    client.connect(broker, port=port)

    client.subscribe(topic=topics.VIDEO_DOWNLOAD)
    client.on_message = on_message
    client.loop_forever()

def main():
    load_dotenv()
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    broker_host = environ.get("MQTT_HOST")
    if broker_host is None:
        logging.fatal("could not load mqtt host")
    else:
        logging.info("MQTT host: %s", broker_host)
    broker_port = environ.get("MQTT_PORT")
    if broker_port is None:
        broker_port = 1883
    else:
        broker_port = int(broker_port)
    logging.info("MQTT port: %d", broker_port)

    broker_username = environ.get("MQTT_USERNAME")
    broker_password = environ.get("MQTT_PASSWORD")
    
    setup_mqtt(broker=broker_host, port=broker_port, broker_username=broker_username, broker_password=broker_password)

if __name__ == '__main__':
    main()