# /usr/bin/python3
# -*- coding:utf-8
import pymongo
import sys
import os
import yaml
from datetime import datetime, date, timedelta, time
from bson.objectid import ObjectId
import json
import requests

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
  # adding cities to show data
  cities = ['Seattle', 'Fort Worth', 'Kissimmee', 'Atlanta', 'Dallas', 'Las Vegas', 'Orlando', 'Renton', 'San Francisco']
  states = ('CA', 'TX', 'NJ', 'NY', 'FL', 'MA', 'WA', 'OR', 'DC', 'VA', 'MD')
  client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
  # states daily data update
  with client:
    collection = client[config['mongo_db_redfin']['room_database']]['Rooms']
    tables = []
    for state in states:
      alive_state = collection.find({'state':state, 'status':'Active'}).count()
      new_online_state = collection.find({'state':state,'_id':{'$lt' : next_DAT_objectid, '$gte' : DAT_objectid} }).count()
      sold_state = collection.find({'state':state, 'last_update':{'$gte':DAY,'$lt':next_DAY}, 'status':'sold'}).count()

      row_state = []
      row.append(state)
      row.append(alive_state)
      row.append(new_online_state)
      row.append(sold_state)
      tables.append(row_state)
      # city data
    for city in cities:

       alive_city = collection.find({'city':city, 'status':'Active'}).count()
       new_online_city = collection.find({'city':city,'_id':{'$lt' : next_DAT_objectid, '$gte' : DAT_objectid} }).count()
       sold_city = collection.find({'city':city, 'last_update':{'$gte':DAY,'$lt':next_DAY}, 'status':'sold'}).count()

       row_city = []
       row.append(city)
       row.append(alive_city)
       row.append(new_online_city)
       row.append(sold_city)
       tables.append(row_city)

  return tables

daily_report = get_daily_report()

# Slack Notification
daily_slack = [i for i in daily_report]
daily_slack.append(['Total', sum([i[1] for i in daily_slack])
                            , sum([i[2] for i in daily_slack])
                            , sum([i[3] for i in daily_slack])
                     ])
title = ['State', 'Alive', 'New', 'Sold']
daily_slack.insert(0, title)
text = ''
text += '```\n'
text += 'Redfin %s(UTC)\n' % str(datetime.now())[:19]
text += '---------------------------------\n'
for row in daily_slack:
  text +=  '|%5s|%9s|%7s|%7s|\n' % (row[0], row[1], row[2], row[3])
text += '---------------------------------\n'
text += '```'
slack_url = config['slack']['url']
slack_headers = {'Content-type': 'application/json'}
slack_data = json.dumps({'text': text})
request = requests.post(url = slack_url,
                        data = slack_data,
                        headers = slack_headers)

# Email Msg Content
sta =['<!DOCTYPE html><html><body><table align="center"><tr><th style="width:80px">State</th><th style="width:80px">Total.alive</th><th style="width:80px">New</th><th style="width:80px">Sold</th></tr>']
# daily_report = get_daily_report()
for i in daily_report:
  sta.append("<tr><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td></tr>".format(i[0],i[1],i[2],i[3]))
sta.append("<tr><td align='center'>Total</td><td align='center'>{}</td><td align='center'>{}</td><td align='center'>{}</td></tr>".format(sum([i[1] for i in daily_report]),sum([i[2] for i in daily_report]),sum([i[3] for i in daily_report])))
sta.append("</table></body></html>")
x = "".join(sta)

content = "{}".format(x)
subject = 'Redfin Daily Report'
mail.send_email(content=content,subject=subject)
