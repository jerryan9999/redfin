#!/usr/bin/python3
# -*- coding: utf-8 -*-

from scrapy.http import Request
from scrapy.spiders import Spider
from redfin.items import RedfinItem
from datetime import datetime, date, timedelta, time
import pymongo
import csv
import re


class RedfinSpider(Spider):
  
  name = "RedfinSpider"
  urls = set()

  def start_requests(self):
    config = self.crawler.settings.get('CONFIG')
    self.client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
    with self.client:
      self.db = self.client[config['mongo_db_redfin']['zipcode_database']]
      self.cursor = self.db['us'].find({}, no_cursor_timeout=True)   # only collection named us
      for index,document in enumerate(self.cursor):
        zipcode = document['_id']
        #if zipcode == '94502':
        url = "https://www.redfin.com/zipcode/"+zipcode
        yield Request(url=url,callback=self.parse_zipcode,meta={'sequence':index,'zipcode':zipcode})

  def parse_zipcode(self,response):
    # parse url like 'https://www.redfin.com/zipcode/98327'
    # get new request whose url links to csv
    index = response.meta['sequence']
    zipcode = response.meta['zipcode']
    csv_url = response.xpath('//a[@id="download-and-save"]/@href').extract_first()
    if csv_url:
      url = 'https://www.redfin.com' + csv_url
      return Request(url=url,callback=self.download_csv,meta={'sequence':index,'zipcode':zipcode})
    else:

      market = re.search(r"market=(.+?)&",response.text).group(1)
      regionId = re.search(r"regionId=(.+?)&",response.text).group(1)
      regionType = re.search(r"regionType=([0-9]+)",response.text).group(1)
      url = "https://www.redfin.com/stingray/api/gis-csv?al=1&market={market}&num_homes=10000&ord=redfin-recommended-asc&page_number=1&region_id={regionId}&region_type={regionType}&sf=1,2,3,5,6,7&status=1&uipt=1,2,3,4,5,6&v=8".format(market=market,regionId=regionId,regionType=regionType)
      return Request(url=url,callback=self.download_csv,meta={'sequence':index,'zipcode':zipcode})

  def download_csv(self,response):
    index = response.meta['sequence']
    zipcode = response.meta['zipcode']
    with open("tmp/"+str(zipcode)+'.csv','w') as f:
      f.write(response.body.decode('utf-8'))

  def parse_csv(self,response):
    # every line of csv is a item
    items_strings = response.body.decode().split('\n')
    for index, line in enumerate(items_strings):      
      if index != 0 and line:
        fields = next(csv.reader(line.splitlines(), skipinitialspace=True))
        if len(fields) == 27:
          item = RedfinItem()
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
          if item['url'] not in self.urls:
            self.urls.add(item['url'])
            yield item


class RedfinSpiderdb(Spider):
  
  name = "RedfinSpiderdb"
  DAY = datetime.combine(date.today(), time(0))
  ONE_DAY = timedelta(days=1)

  def start_requests(self):
    one_day = timedelta(days=1)
    config = self.crawler.settings.get('CONFIG')
    self.client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
    with self.client:
      self.db = self.client[config['mongo_db_redfin']['room_database']]
      self.collection = self.db['Rooms']
      cursor = self.collection.find({"last_update":{"$nin":[self.DAY]}, "history.date":{"$gte":self.DAY-self.ONE_DAY, "$lt":self.DAY}, "status":{"$ne":"sold"}},{'url':1,'redfin_id':1})
      for item in cursor:
        url = item['url']
        self.logger.info("Process url:{}".format(url))
        yield Request(url=url,callback=self.parse_web,meta={'item':item})

  def parse_web(self, response):
    item = response.meta['item']
    status = response.xpath('//span[@class="status-container"]/span/span[2]/div/span/text()').extract_first() \
              or response.xpath('//span[@class="status-container"]/span/span[2]/text()').extract_first()
    if status and 'sold' in status.lower():
      item['status'] = 'sold'
      item['sold_date'] = response.xpath('//div[contains(@class,"home-sash-container large")]/div/div/text()').extract_first()
    else:
      item['status'] = status
    item['days_on_market'] = response.xpath('//div[@class="HomeBottomStats"]/div[1]/div[1]/span[3]/span[2]/text()').re(r'[0-9]+')
    item['price'] = response.xpath('//div[contains(@class,"HomeMainStats home-info")]/div[1]/div[1]/text()').extract_first() \
               or response.xpath('//div[contains(@class,"HomeMainStats home-info")]/div/div/div/span[2]/text()').extract_first()
    item['last_update'] = datetime.combine(date.today(), time(0))
    return item


