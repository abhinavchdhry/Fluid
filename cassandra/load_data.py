from cassandra.cluster import Cluster
import json
import redis

cluster = Cluster()
session = cluster.connect()

r = redis.StrictRedis(host='10.0.0.10', port=6379, db=0)

# Main code
query = """SELECT * FROM ads.ads_table"""
results = session.execute(query)

REDIS_TABLE_NAME = "REDIS_ADS_TABLE"

count = 0
AD_PREFIX = "AD_KEY_PREFIX_"

### Ad IDs are prefixed with string above to be used as keys in REDIS_ADS_TABLE
for ad_row in results:
	r.lpush(AD_PREFIX + ad_row.id, ad_row.title)
	r.lpush(AD_PREFIX + ad_row.id, ad_row.body)
	r.lpush(AD_PREFIX + ad_row.id, ad_row.tags)

	r.lpush(REDIS_TABLE_NAME, AD_PREFIX + ad_row.id)
	count += 1

print("Successfully loaded " + str(count) + " rows into REDIS!")
