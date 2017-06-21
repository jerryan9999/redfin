import os
import csv
from datetime import datetime, date, timedelta, time
import pymongo
import yaml
import re


with open("config.yml") as f:
  config = yaml.load(f)

redfin = (config['mongo_db_redfin']['hosts'],config['mongo_db_redfin']['room_database'])

client = pymongo.MongoClient(redfin[0])
db = client[redfin[1]]
collection = db["Rooms"]

cursor = collection.find(modifiers={"snapshot":True})
cursor.batch_size(1000)

bulk = collection.initialize_unordered_bulk_op()
counter = 0

def parse_csv(response):
  # every line of csv is a item
  items_strings = response.split('\n')
  for index, line in enumerate(items_strings):      
    if index != 0 and line:
      fields = next(csv.reader(line.splitlines(), skipinitialspace=True))
      if len(fields) == 27:
        item = dict()
        item['sale_listing'] = fields[0]
        item['sold_date'] = fields[1]
        item['property_type'] = fields[2]
        item['address'] = fields[3]
        item['city'] = fields[4]
        item['state'] = fields[5]
        item['zipcode'] = fields[6]
        item['price'] = fields[7]
        item['beds'] = fields[8]
        item['baths'] = fields[9]
        item['location'] = fields[10]
        item['square_feet'] = fields[11]
        item['lot_size'] = fields[12]
        item['year_build'] = fields[13]
        item['days_on_market'] = fields[14]
        item['square_feet_price'] = fields[15]
        item['hoa_month'] = fields[16]
        item['status'] = fields[17]
        item['url'] = fields[20]
        item['redfin_id'] = re.search(r'home/([0-9]+)',item['url']).group(1)
        item['source'] = fields[21]
        item['mls'] = fields[22]
        item['latitude'] = fields[25]
        item['longitude'] = fields[26]
        item['history'] = [{
                              'date' : datetime.today(),
                              'price' : fields[7],
                              'status' : fields[17]
                          }]
        item['initial_date'] = datetime.today()
        item['last_update'] = datetime.combine(date.today(), time(0))
        if item['url'] not in processed_urls:
          processed_urls.add(item['url'])
          yield item

def process_item(item):
  global bulk,counter
  # db.Rooms.creatIndex({'mls':1,'zipcode':1}) 记得创建索引
  update = {k:v for k,v in item.items() if k!='history'}
  history = {
            'date':datetime.today(),
            'price':item['price'],
            'status':item['status']
  }
  bulk.find({'redfin_id':item['redfin_id']}).upsert().update_one({'$set': update, '$push':{'history':history}})

  counter+=1
  if counter % 1000 ==0:
    print("bulk updating #"+str(counter))
    bulk.execute()
    bulk = collection.initialize_unordered_bulk_op()

if __name__ == '__main__':
  # get all the files to a list
  files = ['tmp/'+f for f in os.listdir("tmp") if f.endswith('csv')]
  processed_urls = set()

  for file in files:
    with open(file) as f:
      for item in parse_csv(f.read()):
        process_item(item)

  if counter%1000!=0:
    print("bulk updating # "+ str(counter))
    bulk.execute()

  client.close()
