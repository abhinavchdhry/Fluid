from flask import Flask
from flask import render_template
from flask import g
from flask import jsonify
from flask import Response
from cassandra.cluster import Cluster
import time
import redis
import json
from random import sample

app = Flask(__name__)

## Setup connection to cassandra
cluster = Cluster(['10.0.0.6', '10.0.0.7'])
session = cluster.connect()

prepared_query = session.prepare("""SELECT body FROM FINAL.MESSAGES WHERE thread_id = ? AND parent_id = ? ALLOW FILTERING""")

prepared_ad_query = session.prepare("""SELECT title, body FROM FINAL.ADS_TABLE WHERE id = ?""")

## Setup connection to Redis
r = redis.StrictRedis(host='10.0.0.4', db=0)


@app.route('/')
def entry():
#	query = """SELECT * FROM FINAL.OUTPUT_TABLE LIMIT 50"""
#	results = session.execute(query)
	threads = r.keys("THREAD_MSG_MAP_*")

	if len(threads) > 25:
		threads = sample(threads, 25)

	out = {}
#	for row in results:
	for thread in threads:
		thread_id = thread[15:]
#		res = session.execute(prepared_query, [thread_id, thread_id])
		msg_obj = r.lindex(thread, 0)
		body = r.lindex(msg_obj, 4)
		out[thread_id] = body
#		for resrow in res:
#			if '[removed]' not in resrow.body and '[deleted]' not in resrow.body:
#				out[thread_id] = resrow.body
#			break

	return render_template('index.html', result=out)

@app.route('/<thread_id>')
def show_thread(thread_id):
	result = session.execute("""SELECT * FROM FINAL.MESSAGES WHERE thread_id = %s""", [thread_id])
	count = 0
	body = {}
	for row in result:
		body[row.id] = row.body
		count += 1
	print("returned " + str(count) + " rows")
	return render_template('thread.html', thread_id=thread_id, results=body)


def comment_stream_source(thread_id):
	subscriber = r.pubsub()
	subscribe_channel = "THREAD_CHANNEL_" + thread_id
	print("Subscribing to: ", subscribe_channel)
	subscriber.subscribe(subscribe_channel)
	for msg in subscriber.listen():
		if msg['type'] == 'message':
			print("msg rcvd...")
			print(msg['data'])
			yield "data: %s\n\n" % msg['data']


@app.route('/comment-stream/<thread_id>')
def comment_stream(thread_id):
	print('Comment stream started...')
	return Response(comment_stream_source(thread_id), mimetype="text/event-stream")


def ad_stream_source(thread_id):
	ad_subscriber = r.pubsub()
	ad_subscribe_channel = "AD_CHANNEL_" + thread_id
	print("Subscribing to: ", ad_subscribe_channel)
        ad_subscriber.subscribe(ad_subscribe_channel)
	for msg in ad_subscriber.listen():
		if msg['type'] == 'message':
#			obj = json.loads(msg['data'])
			matched_ad_id = msg['data']
			print("Matched ad: " + matched_ad_id)
#			res = session.execute(prepared_ad_query, [matched_ad_id])
#			for row in res:
#				print("Matching row found...")
#				print("title: " + row.title)
#				print("body: " + row.body)
#				break
			title = r.lindex('AD_KEY_PREFIX_' + matched_ad_id, 0)
			body = r.lindex('AD_KEY_PREFIX_' + matched_ad_id, 1)
			yield "data: {\"title\":\"" + title + "\", \"body\":\"" + body + "\"}\n\n"


@app.route('/ad-stream/<thread_id>')
def ad_stream(thread_id):
	print("Ad stream started...")
	return Response(ad_stream_source(thread_id), mimetype="text/event-stream")

#@app.before_request
#def before_request():
	## Setup connection to cassandra
#	g.cluster = Cluster(['10.0.0.6', '10.0.0.7'])
#	g.session = g.cluster.connect()

#@app.after_request
#def after_request(response):
	# Shutdown the session
#	g.cluster.shutdown()
#	return response

if __name__ == '__main__':
	app.debug = True
	app.run(host='0.0.0.0', threaded=True)
