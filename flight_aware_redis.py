#!/usr/bin/env python3
import sys, os.path
from config import *
import redis
import logging
import py1090 
import datetime
import paho.mqtt.publish as publish
from py1090.helpers import distance_between
from py1090 import FlightCollection
import daemon

# configure a file logger
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
log_file = os.path.join(LOG_DIR, 'flight_aware_redis.log')

FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, filename=log_file)
logging.info("Connecting to %s:%d", redis_host,redis_port)
r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True, 
socket_timeout=2.0)

CALL_SIGNS = {}
FLIGHTS = FlightCollection()


def _dump_bool(value):
    if value == True:
        return 1
    else:
        return 0

def to_record(message):
    return_dict = {}
    for k,v in message.__dict__.items():
        if isinstance(v, datetime.datetime):
            v = str(v)
        if v is None:
            continue
        if isinstance(v, bool):
            v = _dump_bool(v)
        return_dict[k] = v
    return return_dict

def publish_rec(message, last_message):
    if message != last_message:
        logging.info("queue message %s on %s", message.strip(), mqtt_topic_name)
        publish.single(mqtt_topic_name, str(message).strip(), hostname=mqtt_host)
        return message
    return last_message

def get_call_sign(hexident):
    flight_rec = FLIGHTS[hexident]
    if flight_rec is None:
        return None
    call_sign = None
    distance = None
    # iterate through messages in reverse order to find the most recent call sign and distance
    for message in reversed(flight_rec.messages):
        if message.callsign:
            call_sign = message.callsign.strip()

        if hasattr(message, "distance"):
            distance = message.distance
        if call_sign and distance is not None:
            break

    return (call_sign, distance)


def record_positions_to_redis(redis_client):
    last_message = None
    msg_count = 0
    distance = 0
    with py1090.Connection(host=fa_host) as connection:
        for line in connection:
            message = py1090.Message.from_string(line)

            if message.latitude and message.longitude:
                distance = distance_between(home_lat, home_long, message.latitude, message.longitude) * 0.000621371
                message.distance = distance
                if distance <= mqtt_distance_max and message.hexident in FLIGHTS:
                    call_sign, dist = get_call_sign(message.hexident)
                    logging.info("Updating %s call_sign='%s'", message.hexident,call_sign) 
                    last_message = publish_rec( call_sign, last_message)
            FLIGHTS.add(message)
            
            call_sign, distance = get_call_sign(message.hexident)
            redis_client.hset(message.hexident, mapping=to_record(message))
            msg_count += 1
            if msg_count % 1000 == 0:
                logging.info("%d %s recorded. last_dist=%d call_sign=%s", msg_count, message.hexident, distance, call_sign)

def run_loop():
    logging.info("Starting to record positions to Redis")
    while True:
        try:
            record_positions_to_redis(r)
        except Exception as e:
            logging.error("Error in recording positions: %s", e)
            continue

if __name__ == "__main__":

    if 'RUNNING_IN_VSCODE' in os.environ:
        logging.info("Running in VSCode environment, not starting daemon")
        run_loop()
        sys.exit(0)
        
    with daemon.DaemonContext():
        pass
        run_loop()
