# /usr/bin/python3
# -*- coding:utf-8
import pymongo
import yaml
from datetime import datetime, date, timedelta, time
from bson.objectid import ObjectId
import json
import requests
import re
from jinja2 import Template

from utils.mail import send_email

CONFIG = yaml.load(open("config.yml"))
DAY = datetime.combine(date.today(), time(0))
# DAY = datetime(2017,5,12)
next_DAY = DAY+ timedelta(days=1)
DAT_objectid = ObjectId.from_datetime(DAY)
next_DAT_objectid = ObjectId.from_datetime(next_DAY)

HEADERS = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2)'}
ZILLOW_STAT_URL = 'https://www.zillow.com/search/GetResults.htm?spt=homes&status=111011&lt=111101&ht=111111&pmf=1&pf=1&sch=100111&search=maplist'
ZILLOW_RANGE_URL = {
  'Seattle':       '&zoom=10&rect=-122589226,47408341,-122100335,47817534&rid=16037',
  'Fort Worth':    '&zoom=9&rect=-97805100,32288874,-96827317,33309298&rid=18172',
  'Kissimmee':     '&zoom=9&rect=-81635628,27947402,-81146737,28482271&rid=18847',
  'Atlanta':       '&zoom=9&rect=-84724846,33514205,-84235955,34018803&rid=37211',
  'Dallas':        '&zoom=9&rect=-97021981,32562729,-96533089,33072843&rid=38128',
  'Las Vegas':     '&zoom=9&rect=-115455666,35893222,-114966774,36383425&rid=18959',
  'Orlando':       '&zoom=9&rect=-81726609,27945279,-80748826,29012343&rid=13121',
  'Renton':        '&zoom=11&rect=-122274914,47365687,-122030468,47570851&rid=13480',
  'San Francisco': '&zoom=11&rect=-122555409,37655149,-122310963,37895040&rid=20330'
}
CITIES = ['Seattle', 'Fort Worth', 'Kissimmee', 'Atlanta', 'Dallas', 'Las Vegas', 'Orlando', 'Renton', 'San Francisco']
# STATES = ('CA', 'TX', 'NJ', 'NY', 'FL', 'MA', 'WA', 'OR', 'DC', 'VA', 'MD')
# Todo: city的统计report修改了格式，states的统计还没修改；email中的统计也没有states

def get_daily_report():
  # adding cities to show data
  client = pymongo.MongoClient(CONFIG['mongo_db_redfin']['hosts'])
  # states daily data update
  with client:
    collection = client[CONFIG['mongo_db_redfin']['room_database']]['Rooms']
    tables = []
    # for state in STATES:
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
    for city in CITIES:
      alive_city = collection.find({'city':city, 'status':'Active'}).count()
      new_online_city = collection.find({'city':city,'_id':{'$lt' : next_DAT_objectid, '$gte' : DAT_objectid} }).count()
      sold_city = collection.find({'city':city, 'last_update':{'$gte':DAY,'$lt':next_DAY}, 'status':'sold'}).count()
      zillow_response = requests.get(ZILLOW_STAT_URL + ZILLOW_RANGE_URL[city], headers=HEADERS).text
      zillow_total_count = re.findall('"totalResultCount":\w+', zillow_response)[0].split(':')[-1]
      
      city_dict = {
        'name':   city,
        'total':  int(zillow_total_count),
        'alive':  alive_city,
        'alive%': alive_city * 100. / float(zillow_total_count) if zillow_total_count else 0.,
        'new':    new_online_city,
        'new%':   new_online_city * 100. / alive_city if alive_city else 0.,
        'sold':   sold_city,
        'sold%':  sold_city * 100. / alive_city if alive_city else 0.
      }
      tables.append(city_dict)

  return tables


# Slack Notification
def slack_notification(daily_report):
  daily_slack = [[i['name'], i['alive'], i['new'], i['sold']] for i in daily_report]
  daily_slack.append(['Total', sum([i[1] for i in daily_slack])
                             , sum([i[2] for i in daily_slack])
                             , sum([i[3] for i in daily_slack])
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
  slack_url = CONFIG['slack']['url']
  slack_headers = {'Content-type': 'application/json'}
  slack_data = json.dumps({'text': text})
  request = requests.post(url = slack_url,
                          data = slack_data,
                          headers = slack_headers)



# 计算与30天前相比的变化 方案1：从mongo里获取history
def get_30d_ago_data_mongo():
  pass # Todo

# 计算与30天前相比的变化 方案2：用30天前的记录
def get_30d_ago_data(daily_report):
  today_30d_ago = date.today() - timedelta(days=30)
  with open('email_report.json', 'r') as f:
    history = json.loads(f.read())
    data_30d_ago = history[str(today_30d_ago)]
    # 按照今天统计的City、State数量计算重新计算30天前的Total
    data_30d_ago = {key: value for key, value in data_30d_ago.items() if key in CITIES} # Todo: and STATES
    data_30d_ago['Total'] = {
      'Total.alive': sum([data['Total.alive'] for city, data in data_30d_ago.items()]),
      'New':         sum([data['New']         for city, data in data_30d_ago.items()]),
      'Sold':        sum([data['Sold']        for city, data in data_30d_ago.items()])
    }
  with open('email_report.json', 'w') as f:
    history[str(date.today())] = {
      dic['name']: {
        'Total.alive': dic['alive'],
        'New':         dic['new'],
        'Sold':        dic['sold']
      } for dic in daily_report
    }
    f.write(json.dumps(history, indent=2))
  return data_30d_ago


# Email Msg Content
def email(daily_report):
  html = '''<!DOCTYPE html><html><body>
    <table align="center">
      <tr>
        <th width=80>               Name          </th>
        <th width=80 align="right"> All           </th>
        <th width=80 align="right"> Total.alive   </th>
        <th width=80 align="right"> alive / all   </th>
        <th width=80 align="right"> DoD (-30d)    </th>
        <th width=80 align="right"> New           </th>
        <th width=80 align="right"> new / alive   </th>
        <th width=80 align="right"> DoD (-30d)    </th>
        <th width=80 align="right"> Sold          </th>
        <th width=80 align="right"> sold / alive  </th>
        <th width=80 align="right"> DoD (-30d)    </th>
      </tr>
      {% for row in daily_report %}
      <tr>
        <td align="center"> {{ row['name'] }} </td>
        <td align="right">  {{ '{:,}'.format(row['total']) }} </td>
        <td align="right">  {{ row['alive'] }} </td>
        <td align="right">  {{ '{:.2f} %'.format(row['alive%']) }} </td>
        {% if row['alive_dod'] >= 0 %}
        <td align="right" style="color: rgb({{ 150 + row['alive_dod']|int }}, 0, 0)"> + {{ '{:.0f} %'.format(row['alive_dod']) }} </td>
        {% else %}
        <td align="right" style="color: rgb(0, {{ 180 - row['alive_dod']|int }}, 0)">   {{ '{:.0f} %'.format(row['alive_dod']) }} </td>
        {% endif %}
        <td align="right">  {{ row['new'] }} </td>
        <td align="right">  {{ row['new%'] }} </td>
        {% if row['new_dod'] >= 0 %}
        <td align="right" style="color: rgb({{ 150 + row['new_dod']|int }}, 0, 0)"> + {{ '{:.0f} %'.format(row['new_dod']) }} </td>
        {% else %}
        <td align="right" style="color: rgb(0, {{ 180 - row['new_dod']|int }}, 0)">   {{ '{:.0f} %'.format(row['new_dod']) }} </td>
        {% endif %}
        <td align="right">  {{ row['sold'] }} </td>
        <td align="right">  {{ row['sold%'] }} </td>
        {% if row['sold_dod'] >= 0 %}
        <td align="right" style="color: rgb({{ 150 + row['sold_dod']|int }}, 0, 0)"> + {{ '{:.0f} %'.format(row['sold_dod']) }} </td>
        {% else %}
        <td align="right" style="color: rgb(0, {{ 180 - row['sold_dod']|int }}, 0)">   {{ '{:.0f} %'.format(row['sold_dod']) }} </td>
        {% endif %}
      </tr>
      {% endfor %}
    </table></body></html>'''
  
  # 计算与30天前相比的变化
  data_30d_ago = get_30d_ago_data(daily_report)
  total = {'name': 'Total'}
  for dic in daily_report:
    for key, value in dic.items():
      if key != 'name':
        total[key] = total[key] + value if key in total else value
  daily_report.append(total)
  for dic in daily_report:
    data = data_30d_ago[dic['name']]
    dic['alive_dod'] = (dic['alive'] - data['Total.alive']) * 100. / data['Total.alive'] if data['Total.alive'] else 0.
    dic['new_dod']   = (dic['new']   - data['New'])         * 100. / data['New']         if data['New']         else 0.
    dic['sold_dod']  = (dic['sold']  - data['Sold'])        * 100. / data['Sold']        if data['Sold']        else 0.

  content = Template(html).render(daily_report=daily_report)
  subject = 'Redfin Daily Report'
  send_email(content=content,subject=subject)

if __name__ == '__main__':
  daily_report = get_daily_report()
  slack_notification(daily_report)
  email(daily_report)
