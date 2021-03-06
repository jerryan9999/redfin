# encoding: -utf-8
import os
import csv
from datetime import datetime, date, timedelta, time
import pymongo
import yaml
import re
from utils.mail import send_email
import requests
import json
from operator import itemgetter


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

  # create a mapping for Redfin property_type and Zillow property_type

  property_mappings = {

  "Condo/Co-op" : "Condo",
  "Multi-Family" : "Multi Family",
  "Residential" : "Single Family",
  "Townhouse" : "Townhouse",
  "Single Family Residential" : "Single Family",
  "Mobile/Manufactured Home" : "Mobile",
  "Multi-Family (2-4 Unit)" : "Multi Family",
  "Vacant Land" : "Lot",
  "Multi-Family (5+ Unit)" : "Multi Family"

  }


  items_strings = response.split('\n')
  for index, line in enumerate(items_strings):
    if index != 0 and line:
      fields = next(csv.reader(line.splitlines(), skipinitialspace=True))
      if len(fields) == 27:
        item = dict()
        item['sale_listing'] = fields[0]
        item['sold_date'] = fields[1]
        if fields[2] and fields[2] not in property_mappings.keys():
          continue
        if property_mappings[fields[2]] == 'Mobile':
            continue
        item['property_type'] = property_mappings[fields[2]]
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
        try:
          float(item['baths'])
          int(item['beds'])
          float(item['square_feet'])
          int(item['year_build'])
        except ValueError:
          continue
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
          business_interested_property(item)
          yield item

def business_interested_property(item):
  # 上线两天以内的新房
  # score大于一定值
  recent_days = 3
  threshold_score = 80
  global new_property
  # 感兴趣的房源筛选
  if item['days_on_market'] and int(item['days_on_market'])<=recent_days and item['status']=="Active" and item['city'] in ['Dallas','Chicago','Seattle']:
    try:
        if item['city']=="Houston":
          if int(item['price']) <=200000 and int(item['price']) >=150000:
            item = attach_score(item)
        elif item['city']=="Seattle":
          if int(item['price']) <=500000:
            item = attach_score(item)
        else:
          item = attach_score(item)
    except Exception as e:
        print(e)
    if item.get('score') and item['score']!="No Data" and int(item['score']) >= threshold_score:
      print("Got better property")
      new_property.append(item)

def attach_score(item):
    # nearby
    # POST http://test.fanglimei.cn/api/rent
    payload = json.dumps({
                "home_id": item['redfin_id'],
                "roomtype": item['property_type'],
                "listing_price": "$"+item['price'],
                "city": item['city'],
                "zipcode": item['zipcode'],
                "beds": int(item['beds']),
                "yearbuilt": int(item['year_build']),
                "size": float(item['square_feet']),
                "addr": item['address'],
                "baths": float(item['baths']),
                "centroid": "{},{}".format(item['latitude'],item['longitude']),
                "source_name": "redfin"
            })
    #print(payload)
    try:
        response = requests.post(
            url="http://test.fanglimei.cn/api/rent",
            headers={
                "Authorization": "eyJhbGciOiJIUzI1NiIsImV4cCI6NDY3NTMxMTI0OCwiaWF0IjoxNTIxNzExMjQ4fQ.eyJyb2xlIjowLCJpZCI6MTEsIm5hbWUiOiJjaHJvbWUifQ.lv2xvvkaWbLxagIydK6k3TC5EVmTCKRF_gcqzEhdnPE",
                "Content-Type": "application/json; charset=utf-8",
            },
            data=payload
        )
        print(response.text)
        if response.status_code==200:
          result = response.json()

          item['score'] = result['Score']
          item['rent'] = result['Suggested_Rent']
          item['appre'] = result['Appr']
          item['ratio'] = result['Ratio']
          item['cap'] = result['Cap']
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
    return item

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
  new_property = []
  files = ['tmp/'+f for f in os.listdir("tmp") if f.endswith('csv')]
  processed_urls = set()

  for file in files:
    with open(file,'r',encoding='utf-8') as f:
      for item in parse_csv(f.read()):
        process_item(item)

  #print("NEW", new_property[0])
  #print("LEN", len(new_property))
  if counter%1000!=0:
    #print("bulk updating # "+ str(counter))
    bulk.execute()

  # sort by two column: city and score
  new_property_sorted = sorted(new_property, key=itemgetter('city', 'score'),reverse=True)

  new_property_atlanta = [i for i in new_property_sorted if i['city']=='Atlanta']
  new_property_orlando = [i for i in new_property_sorted if i['city']=='Orlando']
  new_property_houston = [i for i in new_property_sorted if i['city']=='Houston']
  new_property_dallas = [i for i in new_property_sorted if i['city']=='Dallas']
  new_property_chicago = [i for i in new_property_sorted if i['city']=='Chicago']
  new_property_chicago = [i for i in new_property_sorted if i['city']=='Chicago']
  new_property_seattle = [i for i in new_property_sorted if i['city']=='Seattle']

  # Email Msg Content
  def send_by_city(sorted_city,subject):
    sta =['<!DOCTYPE html><html><body><table align="center"><tr><th style="width:80px">City</th><th style="width:80px">Addr</th><th style="width:80px">Type</th><th style="width:40px">Price</th><th style="width:40px">Bed</th><th style="width:40px">Bath</th><th style="width:40px">Sqrt</th><th style="width:40px">YearBuilt</th><th style="width:40px">Onmarket</th><th style="width:80px">Url</th><th style="width:40px">WeHomeScore</th> <th style="width:40px">WeHomeRent</th> <th style="width:40px">CapRate</th><th style="width:80px">Appreciation</th><th style="width:80px">CashReturn</th></tr>']

    for p in sorted_city:
      sta.append(
        "<tr><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td aligh='center'>{}</td><td align='center'>{}</td></tr>".format(p["city"],p['address'],p['property_type'],p['price'],p['beds'],p['baths'],p['square_feet'],p['year_build'],p['days_on_market'],p['url'],p['score'],p['rent'],p['cap'],p['appre'],p['ratio'])

      )
    sta.append("</table></body></html>")

    x = "".join(sta)

    content = "{}".format(x)

    send_email(content=content, subject=subject)

  #if len(new_property_others)>0:
  #  send_by_city(sorted_city=new_property_others,subject="New Coming Property")
  if len(new_property_atlanta)>0:
    send_by_city(sorted_city=new_property_atlanta,subject="New Coming Property-Atlanta")
  if len(new_property_orlando)>0:
    send_by_city(sorted_city=new_property_orlando,subject="New Coming Property-Orlando")
  if len(new_property_houston)>0:
    send_by_city(sorted_city=new_property_houston,subject="New Coming Property-Houston")
  if len(new_property_dallas)>0:
    send_by_city(sorted_city=new_property_dallas,subject="New Coming Property-Dallas")
  if len(new_property_chicago)>0:
    send_by_city(sorted_city=new_property_chicago,subject="New Coming Property-Chicago")
  if len(new_property_seattle)>0:
    send_by_city(sorted_city=new_property_seattle,subject="New Coming Property-Seattle")


  client.close()
