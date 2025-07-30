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

from collection import FlightNotificationCollection
from config import CONFIG, redact_url_password

# configure a file logger

FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logger = None

FLIGHTS = FlightNotificationCollection()
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
        logger.info("queue message %s on %s", message.strip(), CONFIG.mqtt_topic_name)
        publish.single(CONFIG.mqtt_topic_name, str(message).strip(), hostname=CONFIG.mqtt_host)
        return message
    return last_message

def record_and_cleanup(max_age=3600, sleep_time=60):
    """Remove flights that have not been updated in the last n minutes."""
    logger.info("Connecting to Redis at %s", redact_url_password(CONFIG.redis_url))
    redis_client = redis.Redis.from_url(CONFIG["REDIS_URL"], decode_responses=True,socket_timeout=2.0)
    while True:
        time.sleep(sleep_time)
        logger.info("Starting cleanup of flight collection. size=%d", len(FLIGHTS))
        time_now = datetime.datetime.now(datetime.timezone.utc)
        time_now = time_now.replace(tzinfo=None)
        remove_list = []
        for flight in FLIGHTS.flights():

            if not flight.messages:
                continue
            last_message = flight.messages[-1]
            redis_client.hset(last_message.hexident, mapping=to_record(last_message))
            flight.last_persist = time.time()
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
                del FLIGHTS[id]
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


def record_positions():
    last_message = None
    msg_count = 0
    distance = 0
    with py1090.Connection(host=CONFIG.fa_host) as connection:
        for line in connection:
            message = py1090.Message.from_string(line)
            if message.on_ground:
                continue
            # continue if there's none of  latitude, longitude or callsign
            if not (message.latitude or message.longitude or message.callsign):
                continue

            if message.callsign:
                FLIGHTS.add(message)
                continue

            if message.latitude and message.longitude:
                distance = distance_between(CONFIG.home_latitude,
                                            CONFIG.home_longitude,
                                            message.latitude,
                                            message.longitude) * MILES_PER_METER

                message.distance = distance
                if distance <= CONFIG.mqtt_distance_max and message.hexident in FLIGHTS:
                        try:
                            flight_rec = FLIGHTS[message.hexident]
                            if flight_rec.notified and (time.time() - flight_rec.notified) < 60:
                                logger.info("Flight %s already notified in the last 60 seconds, skipping", message.hexident)
                                continue
                        except KeyError:
                            continue
                        except Exception:
                            logger.exception("Error retrieving flight record for %s", message.hexident)

                        call_sign, dist = get_call_sign(message.hexident)
                        logger.info("Updating %s call_sign='%s'", message.hexident,call_sign)
                        last_message = publish_rec( call_sign, last_message)
                        flight_rec.notified = time.time()

                FLIGHTS.add(message)

            msg_count += 1
            if msg_count % 10000 == 0:
                call_sign, distance = get_call_sign(message.hexident)
                logging.info("%d %s recorded. last_dist=%0.2f call_sign=%s", msg_count, message.hexident, distance, call_sign)

def liveness_message():
    """
    Publish a liveness message to MQTT twice a day at 9 AM and 9 PM
    :return:
    """
    while True:
        now = datetime.datetime.now()
        if now.hour == 8 or now.hour == 20:
            publish.single(CONFIG.mqtt_topic_name, "{} Flts".format(len(FLIGHTS)), hostname=CONFIG.mqtt_host)
            logger.info("Published liveness message")
            time.sleep(3600)  # wait for an hour before checking again
        else:
            time.sleep(60)
            # check every minute

def run_loop():
    global logger
    # reset any existing logger
    logging.getLogger().handlers = []
    # setup logging after daemon context is created
    logging.basicConfig(level=logging.INFO, format=FORMAT, filename=CONFIG.log_filename)
    logger = logging.getLogger(__name__)

    logger.info("Starting to record positions from FlightAware and publish to Redis and MQTT")
    # create a background thread to execute cleanup_flight_collection

    cleanup_thread = threading.Thread(target=record_and_cleanup, daemon=True)
    cleanup_thread.start()

    liveness_thread = threading.Thread(target=liveness_message, daemon=True)
    liveness_thread.start()

    while True:
        try:
            record_positions()
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
