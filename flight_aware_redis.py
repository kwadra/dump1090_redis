#!/usr/bin/env python3
import sys, os.path
from config import *
import redis
import logging
import py1090 
import datetime
FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logging.info("Connecting to %s:%d", redis_host,redis_port)
r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True, 
socket_timeout=2.0)

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

def record_positions_to_redis(redis_client):
    with py1090.Connection(host=fa_host) as connection:
        for line in connection:
            message = py1090.Message.from_string(line)
            redis_client.hset(message.hexident, mapping=to_record(message))
            logging.info("Updating %s", message.hexident)

if __name__ == "__main__":
    while True:
        try:
            record_positions_to_redis(r)
        except Exception as e:
            pass
