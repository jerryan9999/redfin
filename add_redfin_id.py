import pymongo
import yaml
import re


with open('config.yml') as f:
  config = yaml.load(f)
client = pymongo.MongoClient(config['mongo_db_redfin']['hosts'])
db = client[config['mongo_db_redfin']['room_database']]
collection = db['Rooms']

cursor = collection.find(modifiers={"snapshot":True})
cursor.batch_size(1000)

index = 0 
count = cursor.count()

bulk = collection.initialize_unordered_bulk_op()
counter = 0

print("Starting")
try:
  for doc in cursor:
    counter +=1
    # print('updating ' + str(counter) + ' of ' + str(count) + ' '+ doc['zpid'])
    try:
      bulk.find({'_id':doc['_id']}).update({ '$set': { 'redfin_id': re.search(r'home/([0-9]+)',doc['url']).group(1)}})
      if counter % 500 ==0 :
        print("bulk updating #"+str(counter))
        bulk.execute()
        bulk = collection.initialize_unordered_bulk_op()
    except:
      print("Error in updating "+ doc['zpid'])

finally:
  if counter%500!=0:
    print("bulk updating # "+ str(counter))
    bulk.execute()

client.close()
