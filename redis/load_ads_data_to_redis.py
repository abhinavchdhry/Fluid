#from cassandra.cluster import Cluster
import json
import redis
import sys
from random import sample

#cluster = Cluster()
#session = cluster.connect()

if len(sys.argv) != 2:
	print("Needs an argument: Number of records to load")
	exit(1)

N = int(sys.argv[1])

r = redis.StrictRedis(host='10.0.0.4', port=6379, db=0)

# Main code
#query = """SELECT * FROM ads.ads_table"""
#results = session.execute(query)

REDIS_TABLE_NAME = "REDIS_ADS_TABLE"

count = 0
AD_PREFIX = "AD_KEY_PREFIX_"

# Load data into memory
f = open('../data/ads/chicago.data', 'r')
ads = [line for line in f]

selected = sample(ads, N)

### Ad IDs are prefixed with string above to be used as keys in REDIS_ADS_TABLE
for ad in selected:
	ad_row = json.loads(ad)
	r.lpush(AD_PREFIX + ad_row['id'], ad_row['title'])
	r.lpush(AD_PREFIX + ad_row['id'], ad_row['body'])
	r.lpush(AD_PREFIX + ad_row['id'], ad_row['tags'])

	r.lpush(REDIS_TABLE_NAME, AD_PREFIX + ad_row['id'])

print("Successfully loaded " + str(N) + " rows into REDIS!")
