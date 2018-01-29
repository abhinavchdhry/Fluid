import redis, json
import threading
import time
import random

r = redis.StrictRedis(host='10.0.0.4')

ads = open('ads.txt', 'r')
msgs = open('messages.txt', 'r')

for msg in msgs:
	print(msg)
	obj = json.loads(msg, strict=False)
	r.lpush('MESSAGE_OBJ_' + obj['id'], str(obj['score']), obj['body'], obj['author'], obj['subreddit_id'], obj['parent_id'], obj['link_id'])
	r.lpush('THREAD_MSG_MAP_' + obj['link_id'], 'MESSAGE_OBJ_' + obj['id'])

	outobj = {'author':obj['author'].title(), 'body':obj['body']}
	r.publish('THREAD_CHANNEL_' + obj['link_id'], json.dumps(outobj))
	break

REDIS_TABLE_NAME = "REDIS_ADS_TABLE"
AD_PREFIX = "AD_KEY_PREFIX_"

for ad in ads:
	ad_row = json.loads(ad, strict=False)
        if 'img' in ad_row:
                r.lpush(AD_PREFIX + ad_row['id'], ad_row['img'])

        r.lpush(AD_PREFIX + ad_row['id'], ad_row['tags'])
        r.lpush(AD_PREFIX + ad_row['id'], ad_row['body'])
        r.lpush(AD_PREFIX + ad_row['id'], ad_row['title'])

        r.lpush(REDIS_TABLE_NAME, AD_PREFIX + ad_row['id'])


DEFAULT_AD_ID = 'c8659909-091d-44eb-89d6-7f89c74a657d'
r.set('MATCH_' + 't3_1v0vw', DEFAULT_AD_ID)


def read_remaining_msgs():
	import redis, json
	import threading
	import time
	from random import uniform

	r = redis.StrictRedis(host='10.0.0.4')

	msgs = open('messages.txt', 'r')
	next(msgs)
	for msg in msgs:
	        obj = json.loads(msg, strict=False)
        	r.lpush('MESSAGE_OBJ_' + obj['id'], str(obj['score']), obj['body'], obj['author'], obj['subreddit_id'], obj['parent_id'], obj['link_id'])
       		r.lpush('THREAD_MSG_MAP_' + obj['link_id'], 'MESSAGE_OBJ_' + obj['id'])

        	outobj = {'author':obj['author'].title(), 'body':obj['body']}
        	r.publish('THREAD_CHANNEL_' + obj['link_id'], json.dumps(outobj))
		time.sleep(2)


def publish_ads():
	import redis, json
	import threading
	import time
	from random import uniform

	r = redis.StrictRedis(host='10.0.0.4')

	newads = ['50305ca2-5959-4462-a238-20d02548595e', 'e8785da1-c4e3-474c-b221-4199562f0dbe', 'e8785da1-c4e3-474c-b221-7481562f0dbe']
	for ad_id in newads:
		time.sleep(7)
		r.set('MATCH_' + 't3_1v0vw', ad_id)
		r.publish('AD_CHANNEL_' + 't3_1v0vw', ad_id)

# Main
for task in [read_remaining_msgs, publish_ads]:
	t = threading.Thread(target=task)
	t.start()
