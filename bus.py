#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import json
import time
from threading import Thread

__mqtt_broker = None          # MQTT client
__on_connect_handler = None   # external handler for a connection event
__on_message_handler = None   # external handler for a message event


def init(server_address, on_connect, on_message):
    global __mqtt_broker, __on_connect_handler, __on_message_handler

    __on_connect_handler = on_connect
    __on_message_handler = on_message

    tmp_client = mqtt.Client('KHomes')
    tmp_client.on_connect = on_connect_mqtt
    tmp_client.on_message = on_message_mqtt
    tmp_client.connect(server_address, 1883, 60)

    __mqtt_broker = tmp_client


def listen():
    __mqtt_broker.loop_forever()


def on_connect_mqtt(client, userdata, flags, rc):
    __mqtt_broker.subscribe('/manager')   # manager input
    __mqtt_broker.subscribe('/nodes/#')   # nodes talk
    __mqtt_broker.subscribe('/data/#')    # data from modules

    global __on_connect_handler
    __on_connect_handler()


def on_message_mqtt(client, userdata, msg: mqtt.MQTTMessage):
    message = msg.payload.decode('utf-8')
    print('[%s] >>[%s]>> %s' % (str(time.ctime()), msg.topic, message))
    # Processing in a separate thread
    message = prepare_module_message(message)
    process_message = Thread(target=__on_message_handler, args=(msg.topic, message))
    process_message.start()


def send(topic: str, message, to_esp8266=False) -> str:
    """
    Send a message to the bus.
    :param topic: topic/channel to be used
    :param message: str/disct(json)
    :param to_esp8266: bool - whether this transmission is intended for ESP8266 -> pack the message
    :return: message sent to the bus
    """
    to_send = message if isinstance(message, str) else json.dumps(message)
    to_send = to_send if not to_esp8266 else to_send.replace('"', '').replace(' ', '')

    print('[%s] <<[%s]<< %s' % (str(time.ctime()), topic, to_send))
    if __mqtt_broker:
        __mqtt_broker.publish(topic, to_send)

    return to_send


def prepare_module_message(message: str) -> str:
    if message.find('"') >= 0:
        return message

    message = message.replace('{', '{"')
    message = message.replace('}', '"}')
    message = message.replace(':', '":"')
    message = message.replace(',', '","')
    message = message.replace('"[', '[')
    message = message.replace(']"', ']')
    return message.replace('}","{', '},{')
