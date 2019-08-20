import requests
import datetime
import csv
import redis
from zipfile import ZipFile

redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
rd = redis.Redis()


yesterday = datetime.datetime.now() - datetime.timedelta(days = 4)
yesterdayDate = yesterday.strftime("%d%m%y")
zipURL = "https://www.bseindia.com/download/BhavCopy/Equity/EQ"+ yesterdayDate +"_CSV.ZIP";
file_url = zipURL;

r = requests.get(file_url, stream=True)

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

def saveToRedis():
    csv_values = csv.DictReader(open('EQ160819.CSV', 'r'))
    for row in csv_values:
        rd.hmset(row['SC_NAME'].rstrip(), dict(row))
saveToRedis()

print(rd.hgetall(dict))
# print (rd.hmset('SC_NAME'))