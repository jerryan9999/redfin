#!/usr/bin/python3
# -*- coding: utf-8 -*-

from scrapy.http import Request
from scrapy.spiders import Spider
from redfin.items import RedfinItem
import pymongo

class RedfinSpider(Spider):
  
  name = "RedfinSpider"

  def config_database(self):
    config = self.crawler.settings.get('CONFIG')
    self.client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
    with self.client:
      self.db = self.client[config['mongo_db_redfin']['room_database']]
      self.cursor = self.db['us'].find()    # only collection named us

  def start_requests(self):
    for document in self.cursor:
      zipcode = document['_id']
      if zipcode == '98327':    # test lock
        url = "https://www.redfin.com/zipcode/"+zipcode
        yield Request(url=url,callback=parse_zipcode)

  def parse_zipcode(self,response):
    # parse url like 'https://www.redfin.com/zipcode/98327'
    # get new request whose url links to csv
    csv_url = response.xpath('      ')
    return Request(url=csv_url,callback=parse_csv)

  def parse_csv(self,response):
    # every line of csv is a item
    items_strings = response.body.split('\n')
    for line in items_strings:
      fields = line.split(',')
      item = RedfinItem()
      item['sale_type'] = fields[0]
      item['price'] = 


      #item['longitude']
      return item
