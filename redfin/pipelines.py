# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import pymongo
from datetime import datetime


logger = logging.getLogger(__name__)

class RedfinRoomPipeline(object):

  collection = 'Rooms'

  def open_spider(self, spider):
    config = spider.settings.get('CONFIG')
    try:
      self.client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
      self.db = self.client[config['mongo_db_redfin']['room_database']]
    except:
      logging.error('database collection faild')
      raise

  def process_item(self, item, spider):
    collection = self.db[self.collection]
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

    return item

  def close_spider(self, spider):
    self.client.close()

    

