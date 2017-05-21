import os
import csv
from datetime import datetime, date, timedelta, time
import pymongo
import yaml


with open("config.yml") as f:
  config = yaml.load(f)

redfin = (config['mongo_db_redfin']['hosts'],config['mongo_db_redfin']['room_database'])

class mongo_database():
  def __init__(self, config):
    self.client = None
    self.config = config
  def __enter__(self):
    host,dbName = self.config
    self.client = pymongo.MongoClient(
      host,
      27017
    )
    return self.client
  def __exit__(self, *args):
    self.client.close()

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
  with mongo_database(redfin) as db:
    collection = db[config['mongo_db_redfin']['room_database']]['Rooms']
    # db.Rooms.creatIndex({'mls':1,'zipcode':1}) 记得创建索引
    count = collection.find({'mls':item['mls'], 'zipcode':item['zipcode']}).count()
    if count == 0:
      collection.insert(item)
    else:
      update = {
                'price':item['price'],
                'status':item['status'], 
                'days_on_market':item['days_on_market'],
                'sold_date':item['sold_date'],
                'last_update':item['last_update']
      }
      history = {
                'date':datetime.today(),
                'price':item['price'],
                'status':item['status']
      }
      collection.update_one({'mls':item['mls'], 'zipcode':item['zipcode']}, {'$set': update, '$push':{'history':history}})

if __name__ == '__main__':
  # get all the files to a list
  files = ['tmp/'+f for f in os.listdir("tmp") if f.endswith('csv')]
  counter = 0
  processed_urls = set()

  for file in files:
    with open(file) as f:
      for item in parse_csv(f.read()):
        process_item(item)
        counter+=1
        print(counter)
