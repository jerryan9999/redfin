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
import re

from utils.mail import send_email

with open("config.yml") as f:
  config = yaml.load(f)

DAY = datetime.combine(date.today(), time(0))
# DAY = datetime(2017,5,12)
next_DAY = DAY+ timedelta(days=1)
DAT_objectid = ObjectId.from_datetime(DAY)
next_DAT_objectid = ObjectId.from_datetime(next_DAY)
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2)'}

zillow_stat_url = 'https://www.zillow.com/search/GetResults.htm?spt=homes&status=111011&lt=111101&ht=111111&pmf=1&pf=1&sch=100111&search=maplist'
zillow_range_url = {
  'Seattle': '&zoom=10&rect=-122589226,47408341,-122100335,47817534&rid=16037',
  'Fort Worth': '&zoom=9&rect=-97805100,32288874,-96827317,33309298&rid=18172',
  'Kissimmee': '&rect=-81635628,27947402,-81146737,28482271&rid=18847',
  'Atlanta': '&rect=-84724846,33514205,-84235955,34018803&rid=37211',
  'Dallas': '&rect=-97021981,32562729,-96533089,33072843&rid=38128',
  'Las Vegas': '&rect=-115455666,35893222,-114966774,36383425&rid=18959',
  'Orlando': '&zoom=9&rect=-81726609,27945279,-80748826,29012343&rid=13121',
  'Renton': '&zoom=11&rect=-122274914,47365687,-122030468,47570851&rid=13480',
  'San Francisco': '&zoom=11&rect=-122555409,37655149,-122310963,37895040&rid=20330'
}

def get_daily_report():
  # adding cities to show data
  cities = ['Seattle', 'Fort Worth', 'Kissimmee', 'Atlanta', 'Dallas', 'Las Vegas', 'Orlando', 'Renton', 'San Francisco']
  #states = ('CA', 'TX', 'NJ', 'NY', 'FL', 'MA', 'WA', 'OR', 'DC', 'VA', 'MD')
  client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
  # states daily data update
  with client:
    collection = client[config['mongo_db_redfin']['room_database']]['Rooms']
    tables = []
    #for state in states:
    #  alive_state = collection.find({'state':state, 'status':'Active'}).count()
    #  new_online_state = collection.find({'state':state,'_id':{'$lt' : next_DAT_objectid, '$gte' : DAT_objectid} }).count()
    #  sold_state = collection.find({'state':state, 'last_update':{'$gte':DAY,'$lt':next_DAY}, 'status':'sold'}).count()

    #  row_state = []
    #  row_state.append(state)
    #  row_state.append(alive_state)
    #  row_state.append(new_online_state)
    #  row_state.append(sold_state)
    #  tables.append(row_state)
    
    # city data
    for city in cities:
      alive_city = collection.find({'city':city, 'status':'Active'}).count()
      new_online_city = collection.find({'city':city,'_id':{'$lt' : next_DAT_objectid, '$gte' : DAT_objectid} }).count()
      sold_city = collection.find({'city':city, 'last_update':{'$gte':DAY,'$lt':next_DAY}, 'status':'sold'}).count()
      zillow_response = requests.get(zillow_stat_url + zillow_range_url[city], headers=headers).content
      zillow_total_count = re.findall('"totalResultCount":\w+', zillow_response)[0].split(':')[-1]
      
      row_city = [
        city,                                                                 # 0. State / City
        int(zillow_total_count),                                              # 1. All property count (Zillow)
        alive_city,                                                           # 2. Total.alive
        alive_city / float(zillow_total_count) if zillow_total_count else 0,  # 3. Total.alive / All_property_count (Z)
        new_online_city,                                                      # 4. New
        new_online_city / 1. / alive_city if alive_city else 0,               # 5. New  / Total.alive
        sold_city,                                                            # 6. Sold
        sold_city / 1. / alive_city if alive_city else 0                      # 7. Sold / Total.alive
      ]
      tables.append(row_city)

  return tables

daily_report = get_daily_report()

# Slack Notification
daily_slack = [i for i in daily_report]
daily_slack.append(['Total', sum([i[2] for i in daily_slack])
                           , sum([i[4] for i in daily_slack])
                           , sum([i[6] for i in daily_slack])
])
title = ['Location', 'Alive', 'New', 'Sold']
daily_slack.insert(0, title)
text = ''
text += '```\n'
text += 'Redfin %s(UTC)\n' % str(datetime.now())[:19]
text += '---------------------------------\n'
for row in daily_slack:
  text +=  '|%15s|%9s|%7s|%7s|\n' % (row[0], row[1], row[2], row[3])
text += '---------------------------------\n'
text += '```'
slack_url = config['slack']['url']
slack_headers = {'Content-type': 'application/json'}
slack_data = json.dumps({'text': text})
request = requests.post(url = slack_url,
                        data = slack_data,
                        headers = slack_headers)

# Email Msg Content
sta =['''<!DOCTYPE html><html><body><table align="center">
          <tr><th width=80>State</th>
              <th width=80 align="right">All</th>
              <th width=80 align="right">Total.alive</th>
              <th width=80 align="right">alive / all</th>
              <th width=80 align="right">New</th>
              <th width=80 align="right"">new / alive</th>
              <th width=80 align="right">Sold</th>
              <th width=80 align="right">sold / alive</th>
          </tr>''']
# daily_report = get_daily_report()
for i in daily_report:
  sta.append('<tr>')
  sta += ['<td align="center">   {}  </td>'.format(i[j]) if j in [0] else               # city
          '<td align="right">{:.2f} %</td>'.format(i[j] * 100.) if j in [3, 5, 7] else  # percentage
          '<td align="right">  {:,}  </td>'.format(i[j]) for j in range(8)]
  sta.append('</tr>')
sta.append('<tr><td align="center">Total</td>')
total_row = ['Total'] + [sum([i[j] for i in daily_report]) for j in range(1, 8)]
total_row[3] = '{:.2f} %'.format(total_row[2] * 100. / total_row[1])   # 3. Total.alive / All_property_count (Z)
total_row[5] = '{:.2f} %'.format(total_row[4] * 100. / total_row[2])   # 5. New  / Total.alive
total_row[7] = '{:.2f} %'.format(total_row[6] * 100. / total_row[2])   # 7. Sold / Total.alive
sta += ['<td align="right">{:,}</td>'.format(total_row[j]) if isinstance(total_row[j], int) else
        '<td align="right">  {}</td>'.format(total_row[j]) for j in range(1, 8)]
sta.append("</tr></table></body></html>")
x = "".join(sta)

content = "{}".format(x)
subject = 'Redfin Daily Report'
send_email(content=content,subject=subject)
