import requests
import datetime
import csv
import io
import os
import redis

redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
rd = redis.Redis()


def saveToRedis():
    rd.flushall()
    csv_values = csv.DictReader(open('EQ160819.CSV', 'r'))
    for row in csv_values:
        rd.hmset(row['SC_NAME'].rstrip(), dict(row))

saveToRedis()
val = rd.hmget('AELOGISGIS ','LAST')
print(val[0])
