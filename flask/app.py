from flask import Flask
from flask import render_template
from flask import g
from flask import jsonify
from flask import Response
from cassandra.cluster import Cluster
import time
import redis
import json

app = Flask(__name__)

## Setup connection to cassandra
cluster = Cluster(['10.0.0.6', '10.0.0.7'])
session = cluster.connect()

prepared_query = session.prepare("""SELECT body FROM FINAL.MESSAGES WHERE thread_id = ? AND parent_id = ? ALLOW FILTERING""")

## Setup connection to Redis
r = redis.StrictRedis(host='10.0.0.4', db=0)


@app.route('/')
def entry():
	query = """SELECT * FROM FINAL.OUTPUT_TABLE LIMIT 50"""
	results = session.execute(query)
	out = {}
	for row in results:
		thread_id = row.thread_id
		res = session.execute(prepared_query, [thread_id, thread_id])
		for resrow in res:
			if '[removed]' not in resrow.body and '[deleted]' not in resrow.body:
				out[thread_id] = resrow.body
			break

	return render_template('index.html', result=out)

@app.route('/<thread_id>')
def show_thread(thread_id):
	result = session.execute("""SELECT * FROM FINAL.MESSAGES WHERE thread_id = %s""", [thread_id])
	print("Returned row!")
	count = 0
	body = {}
	for row in result:
		body[row.id] = row.body
		count += 1
	print("returned " + str(count) + " rows")
	return render_template('thread.html', thread_id=thread_id, results=body)

def stream_source():
	for i in range(1000):
		time.sleep(1)
		print("data: msg" + str(i))
		yield "data: msg%s\n\n" % str(i)

@app.route('/stream')
def stream_update():
	return Response(stream_source(), mimetype="text/event-stream")

def comment_stream_source(thread_id):
	subscriber = r.pubsub()
	subscribe_channel = "THREAD_" + thread_id
	print("Subscribing to: ", subscribe_channel)
	subscriber.subscribe(subscribe_channel)
	for msg in subscriber.listen():
		if msg['type'] == 'message':
			print("msg rcvd...")
			print(msg['data'])
			yield "data: %s\n\n" % msg['data']


@app.route('/comment-stream/<thread_id>')
def comment_stream(thread_id):
	return Response(comment_stream_source(thread_id), mimetype="text/event-stream")

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
	app.run(host='0.0.0.0')
