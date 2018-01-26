import redis

r = redis.StrictRedis(host="10.0.0.10", port=6379, db=0)

r.flushall()
