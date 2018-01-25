import redis

r = redis.StrictRedis(host="10.0.0.10", port=6379, db=0)

for key in r.keys():
	if key != "REDIS_ADS_TABLE" and "AD_KEY_PREFIX" not in key:
		r.delete(key)
