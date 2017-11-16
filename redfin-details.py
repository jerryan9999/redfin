# encoding=utf8
import logging
import re
import threading
from Queue import Queue

import datetime
import pymongo
import requests
import time

import yaml

filehandler = logging.FileHandler('notification.log')


def create_logger(level=logging.INFO):
  """Create a logger according to the given settings by jerry"""

  logger = logging.getLogger("IPlogger")
  logger.setLevel(level)
  formatter = logging.Formatter('%(asctime)s  %(filename)s  %(levelname)s - %(message)s',
                                datefmt='%a, %d %b %Y %H:%M:%S', )
  filehandler.setFormatter(formatter)
  logger.addHandler(filehandler)
  return logger


config_file = yaml.load(open('config.yml'))
#config_file={'mongo_db_redfin':{'hosts':'localhost','room_database':'downloads'},'mongo_db_proxy':{'hosts': 'localhost','proxy_database': 'ProxyPool','proxy_collection': 'IPs'}}
#test
def get_img(html):
  result = []
  t = re.findall('"nonFullScreenPhotoUrl\\\\":\\\\"(.*?)\\\\"', html, re.S)
  for each in t:
    each = each.replace('\\u002F', '/')
    result.append(each)
  return result


def get_remark(html):
  return re.findall('"marketingRemark\\\\":\\\\"(.*?)\\\\"', html, re.S)[0].replace('\\\\', '\\').decode(
    'unicode-escape')


def get_property(html):
  global result
  from HTMLParser import HTMLParser
  result = ''

  class MyHTMLParser(HTMLParser):
    def handle_data(self, data):
      global result
      result = result + data + ' '

  parser = MyHTMLParser()
  k = re.findall(
    'AmenitiesInfoSection " data-reactid="3">(.*?)</div></div></div></div></section></div></div></div><script>', html,
    re.S)[
    0]
  parser.feed(k)
  return result


def get_history(html):
  global result, result1
  global t
  t = 0
  from HTMLParser import HTMLParser
  result = []
  result1 = []

  class MyHTMLParser(HTMLParser):
    def handle_data(self, data):
      global result
      result.append(data)

  parser = MyHTMLParser()
  k = re.findall('Property History</span>(.*?)For completeness, Redfin often', html, re.S)[0].replace('\\\\', '\\')
  parser.feed(k)
  result = result[7:]
  result2 = []
  result3 = []
  for each in result:
    if t % 5 == 0:
      result2.append(result1)
      result1 = []
    result1.append(each.replace(u'\u2014', '-'))
    t = t + 1
  for each in result2[1:]:
    result3.append({'Date': each[0],
                    'Event': each[1],
                    'Source': each[2],
                    'Price': each[3],
                    'Appreciation': each[4]})
  return result3


class Redfin:
  def __init__(self):
    self.proxy = Queue()
    self.proxies = {}
    self.ip_change = 0
    self.success = 0
    self.start_proxy()
    self.sche = Queue()
    time.sleep(1)
    self.change()
  def change(self):
    try:
      pr = self.proxy.get()
      self.ip_change += 1
      self.proxies = {'http': pr, 'https': pr}
    except:
      time.sleep(60)
  def get_proxy(self):
    while True:
      if self.proxy.qsize() < 5:
        # t = requests.get(
        #   'http://http-api.taiyangruanjian.com/getip?num=10&type=1&pro=0&city=0&yys=0&port=11&pack=7149&ts=0&ys=0&cs=0&lb=1&sb=0&pb=5&mr=1').text
        # for each in t.split('\r\n')[:-1]:
        #   self.proxy.put('http://' + each)
        conn_mongo = pymongo.MongoClient(config_file['mongo_db_proxy']['hosts'], 27017)
        with conn_mongo:
          db = conn_mongo[config_file['mongo_db_proxy']['proxy_database']][config_file['mongo_db_proxy']['proxy_collection']]
          for each in db.find():
            self.proxy.put('http://'+each['_id'])


  def start_proxy(self):
    threading.Thread(target=self.get_proxy).start()

  def get_html(self, url):
    headers = {'user-agent':
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'}
    # url = 'https://www.redfin.com/WA/Edgewood/3113-87th-Avenue-Ct-E-98371/home/2911874'
    for x in range(500000):
      try:
        p = requests.get(url,headers=headers,timeout=10,proxies=self.proxies,verify=False)
        if p.status_code == 500:
          self.success += 1
          break
        if p.status_code != 200:
          raise Exception('123')
        if 'It looks like our usage analysis algorithms' in p.text:
          raise Exception('Change!')
        self.success += 1
        break
      except Exception as err:
        try:
          #print p.text
          #print p.status_code,self.proxies
          pass
        except:
          #print err
          pass
        self.change()
    return p.text


def sche():
  t=Redfin()
  while source.qsize() > 0:
    res = source.get()
    #print res
    url = res[0]
    html=t.get_html(url)
    try:
      resultk.put([{'img':get_img(html),
      'property':get_property(html),
      'remark':get_remark(html),
      'property_history':get_history(html)
      },url])
      #print t.success,t.ip_change
    except:
      #print url
      pass

def tomongo():
  conn_mongo = pymongo.MongoClient(config_file['mongo_db_redfin']['hosts'], 27017)
  with conn_mongo:
    db=conn_mongo[config_file['mongo_db_redfin']['room_database']]['Rooms']
    while True:
      if resultk.qsize()>0:
        k=resultk.get()
        db.update({"url": k[1]}, {"$set":k[0]})


tm=(datetime.datetime.now()- datetime.timedelta(days=1)) #YESTERDAY
source = Queue()
conn_mongo = pymongo.MongoClient(config_file['mongo_db_redfin']['hosts'], 27017)
resultk=Queue()
with conn_mongo:
  db = conn_mongo[config_file['mongo_db_redfin']['room_database']]
  for each in db['Rooms'].find({'last_update':{'$gte':tm}}):
    url = each['url']
    id = each['_id']
    source.put([url, id])
threading.Thread(target=sche).start()
threading.Thread(target=tomongo).start()

