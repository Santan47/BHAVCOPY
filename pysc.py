import requests
import datetime
import csv
import io
import os
import redis
from zipfile import ZipFile
from bs4 import BeautifulSoup

yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
yesterdayDate = yesterday.strftime("%d%m%y")
zipURL = "https://www.bseindia.com/download/BhavCopy/Equity/EQ"+ yesterdayDate +"_CSV.ZIP";
file_url = zipURL;

r = requests.get(file_url, stream=True)
# soup = BeautifulSoup(r, 'lxml')

with open("Bhav.ZIP", "wb") as zip:
    for chunk in r.iter_content(chunk_size=1024):

        if chunk:
            zip.write(chunk)

with ZipFile("Bhav.ZIP", 'r') as zip:
    zip.extractall()

# with open('EQ160819.CSV', 'r') as csvFile:
#     reader = csv.DictReader(csvFile)
#     for row in reader:
#         print(row['SC_NAME'].rstrip())

redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
rd = redis.Redis()

def saveToRedis():
    rd = redis.Redis()
    csv_values = csv.DictReader(open('EQ210819.CSV', 'r'))
    for row in csv_values:
        rd.hmset(row['SC_NAME'].rstrip(), dict(row))
    # r.set('scrape_date', str(datetime.today().date().day))

# saveToRedis()
# print(rd.hmget('ICIC MMP3AUG', 'LAST')[0])

def stockByName(name):
    results = []
    rd = redis.Redis()
    saveToRedis()
    for equity in rd.scan_iter(match='*'+str(name).upper()+'*'):
        results.append(rd.hgetall(equity))
    return results

def topTenStocks():
    results = []
    rd = redis.Redis()
    saveToRedis()
    keys = rd.keys('*')
    # keys.remove('scrape_date')
    for equity in keys:
        results.append(rd.hgetall(equity))
    newlist = sorted(results, key=lambda x: (float(x['PREVCLOSE'])-float(x['CLOSE']))/float(x['LAST']))
    return newlist[:10]


# call functions
# print (topTenStocks());
print (stockByName('SGBJULY27'))