from flask import Flask
from flask import render_template
from flask import g
from flask import jsonify
from flask import Response
from cassandra.cluster import Cluster
import time

app = Flask(__name__)

## Setup connection to cassandra
cluster = Cluster(['10.0.0.6', '10.0.0.7'])
session = cluster.connect()

prepared_query = session.prepare("""SELECT body FROM FINAL.MESSAGES WHERE thread_id = ? AND parent_id = ? ALLOW FILTERING""")

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
	result = session.execute("""SELECT * FROM FINAL.MESSAGES WHERE thread_id = %s""", ["thread_id"])
	print("Returned row!")
	return render_template('thread.html', thread=thread_id, results=result)

def stream_source():
	for i in range(1000):
		time.sleep(1)
		print("data: msg" + str(i))
		yield "data: msg%s\n\n" % str(i)

@app.route('/stream')
def stream_update():
	return Response(stream_source(), mimetype="text/event-stream")

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
