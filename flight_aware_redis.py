#!/usr/bin/env python3
import gc
import sys, os.path
import threading
import time

import redis
import logging
import py1090 
import datetime
import paho.mqtt.publish as publish
from py1090.helpers import distance_between
from py1090 import FlightCollection
import daemon
from config import CONFIG, redact_url_password

# configure a file logger

FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logger = logging.getLogger(__name__)

FLIGHTS = FlightCollection()
MILES_PER_METER = 0.000621371


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
        logging.info("queue message %s on %s", message.strip(), CONFIG.mqtt_topic_name)
        publish.single(CONFIG.mqtt_topic_name, str(message).strip(), hostname=CONFIG.mqtt_host)
        return message
    return last_message

def cleanup_flight_collection(max_age=3600):
    """Remove flights that have not been updated in the last n minutes."""
    while True:
        time.sleep(max_age / 2)
        logger.info("Starting cleanup of flight collection. size=%d", len(FLIGHTS))
        time_now = datetime.datetime.now(datetime.timezone.utc)
        time_now = time_now.replace(tzinfo=None)
        remove_list = []
        for flight in FLIGHTS:
            if not flight.messages:
                continue
            last_message = flight.messages[-1]
            age =  (time_now - last_message.generation_time).total_seconds()
            if age > max_age:
                logger.info("Removing flight %s from collection", flight.hexident)
                remove_list.append(flight.hexident)
            # trim messages to the newest 50
            if len(flight.messages) > 50:
                flight.messages = flight.messages[-50:]
        # remove from dictionary
        for id in remove_list:
            try:
                del FLIGHTS._dictionary[id]
            except KeyError:
                logger.warning("Flight %s not found in FLIGHTS collection", id)
        gc.collect()


def get_call_sign(hexident):
    flight_rec = FLIGHTS[hexident]
    if flight_rec is None:
        return None
    call_sign = None
    distance = -1
    # iterate through messages in reverse order to find the most recent call sign and distance
    for message in reversed(flight_rec.messages):
        if message.callsign:
            call_sign = message.callsign.strip()

        if hasattr(message, "distance"):
            distance = message.distance
        if call_sign and distance != -1:
            break

    return (call_sign, distance)


def record_positions_to_redis(redis_client):
    last_message = None
    msg_count = 0
    distance = 0
    with py1090.Connection(host=CONFIG.fa_host) as connection:
        for line in connection:
            message = py1090.Message.from_string(line)
            if message.on_ground:
                continue

            if message.latitude and message.longitude:
                distance = distance_between(CONFIG.home_latitude,
                                            CONFIG.home_longitude,
                                            message.latitude,
                                            message.longitude) * MILES_PER_METER

                message.distance = distance
                if distance <= CONFIG.mqtt_distance_max and message.hexident in FLIGHTS:
                    call_sign, dist = get_call_sign(message.hexident)
                    logger.info("Updating %s call_sign='%s'", message.hexident,call_sign)
                    last_message = publish_rec( call_sign, last_message)
            FLIGHTS.add(message)
            # publish to redis
            redis_client.hset(message.hexident, mapping=to_record(message))
            msg_count += 1
            if msg_count % 1000 == 0:
                call_sign, distance = get_call_sign(message.hexident)
                logging.info("%d %s recorded. last_dist=%0.2f call_sign=%s", msg_count, message.hexident, distance, call_sign)

def run_loop():
    # setup logging after daemon context is created
    logging.basicConfig(level=logging.INFO, format=FORMAT, filename=CONFIG.log_filename)
    logging.info("Starting to record positions to Redis")
    # create a background thread to execute cleanup_flight_collection

    cleanup_thread = threading.Thread(target=cleanup_flight_collection, daemon=True)
    cleanup_thread.start()

    logging.info("Connecting to Redis at %s", redact_url_password(CONFIG.redis_url))
    r = redis.Redis.from_url(CONFIG["REDIS_URL"], decode_responses=True,socket_timeout=2.0)

    while True:
        try:
            record_positions_to_redis(r)
        except Exception as e:
            logging.error("Error in recording positions: %s", e)
            continue

if __name__ == "__main__":

    if 'RUNNING_IN_IDE' in os.environ:
        logging.info("Running in IDE environment, not starting daemon")
        run_loop()
        sys.exit(0)
        
    with daemon.DaemonContext():
        run_loop()
