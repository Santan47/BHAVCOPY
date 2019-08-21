import requests
import datetime
import csv
import io
import os
import redis
from zipfile import ZipFile
from bs4 import BeautifulSoup

yesterday = datetime.datetime.now() - datetime.timedelta(days = 4)
yesterdayDate = yesterday.strftime("%d%m%y")
zipURL = "https://www.bseindia.com/download/BhavCopy/Equity/EQ"+ yesterdayDate +"_CSV.ZIP";
file_url = zipURL;

r = requests.get(file_url, stream=True)
# soup = BeautifulSoup(r, 'lxml')

with open("Bhav.ZIP", "wb") as zip:
    for chunk in r.iter_content(chunk_size=1024):

        if chunk:
            zip.write(chunk)

with ZipFile("EQ160819_CSV.ZIP", 'r') as zip:
    zip.extractall()

# with open('EQ160819.CSV', 'r') as csvFile:
#     reader = csv.DictReader(csvFile)
#     for row in reader:
#         print(row['SC_NAME'].rstrip())

redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
rd = redis.Redis()

def saveToRedis():
    csv_values = csv.DictReader(open('EQ160819.CSV', 'r'))
    for row in csv_values:
        rd.hmset(row['SC_NAME'].rstrip(), dict(row))
    # r.set('scrape_date', str(datetime.today().date().day))

saveToRedis()
print(rd.hmget('ICIC MMP3AUG','LAST')[0])