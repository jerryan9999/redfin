# /usr/bin/python3
# -*- coding:utf-8
import pymongo
import sys
import os
import yaml
from datetime import datetime, date, timedelta, time
from bson.objectid import ObjectId

utils_path = os.getcwd()+"/utils/"    # TODO: relative import within package
sys.path.append(utils_path)

import mail

with open("config.yml") as f:
  config = yaml.load(f)

DAY = datetime.combine(date.today(), time(0))
#DAY = datetime(2017,5,12)
next_DAY = DAY+ timedelta(days=1)
DAT_objectid = ObjectId.from_datetime(DAY)
next_DAT_objectid = ObjectId.from_datetime(next_DAY)

def get_daily_report():
  states = ('CA', 'TX', 'NJ', 'NY', 'FL', 'MA', 'WA', 'OR', 'DC', 'VA', 'MD')
  client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
  with client:
    collection = client[config['mongo_db_redfin']['room_database']]['Rooms']
    tables = []
    for state in states:
      alive = collection.find({'state':state, 'status':{'$ne':'sold'}}).count()
      new_online = collection.find({'state':state,'_id':{'$lt' : next_DAT_objectid, '$gte' : DAT_objectid} }).count()
      sold = collection.find({'state':state, 'last_update':{'$gte':DAY,'$lt':next_DAY}, 'status':'sold'}).count()

      row = []
      row.append(state)
      row.append(alive)
      row.append(new_online)
      row.append(sold)
      tables.append(row)
  return tables

# Email Msg Content
sta =['<!DOCTYPE html><html><body><table align="center"><tr><th style="width:80px">State</th><th style="width:80px">Total.alive</th><th style="width:80px">New</th><th style="width:80px">Sold</th></tr>']
daily_report = get_daily_report()
for i in daily_report:
  sta.append("<tr><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td></tr>".format(i[0],i[1],i[2],i[3]))
sta.append("<tr><td align='center'>Total</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td></tr>".format(sum([i[1] for i in daily_report]),sum([i[2] for i in daily_report]),sum([i[3] for i in daily_report])))
sta.append("</table></body></html>")
x = "".join(sta)

content = "{}".format(x)
subject = 'Redfin Daily Report'
mail.send_email(content=content,subject=subject)
