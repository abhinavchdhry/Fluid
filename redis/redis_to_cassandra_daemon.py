import redis
from cassandra.cluster import Cluster
import json

c = Cluster(["10.0.0.6", "10.0.0.7"])
s = c.connect()

r = redis.StrictRedis(host='10.0.0.4', db=0)

pubsub = r.pubsub()
pubsub.subscribe('THREAD_AD_MATCH')

BATCH_SIZE = 10

counter = 0
batch_insert = ""

prepared_insert_stmt = s.prepare("""INSERT INTO FINAL.OUTPUT_TABLE (thread_id, matched_ad_id) VALUES (?, ?)""")

print("Listening...")
for msg in pubsub.listen():
	if 'data' in msg and'type' in msg and msg['type'] == 'message':
		out = json.loads(msg['data'])
		s.execute(prepared_insert_stmt, [out['thread_id'], out['matched_ad_id']])
