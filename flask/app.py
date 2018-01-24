from flask import Flask
from flask import render_template
from flask import g
from flask import jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)

## Setup connection to cassandra
#cluster = Cluster(['10.0.0.6', '10.0.0.7'])
#session = cluster.connect()

@app.route('/')
def entry():
	query = """SELECT * FROM FINAL.OUTPUT_TABLE LIMIT 20"""
	results = g.session.execute(query)
	thread_ids = [_id for _id in results]
	return render_template('index.html', result=thread_ids)

@app.route('/<thread_id>')
def show_thread(thread_id):
	return render_template('thread.html', thread=thread_id)

@app.before_request
def before_request():
	## Setup connection to cassandra
	g.cluster = Cluster(['10.0.0.6', '10.0.0.7'])
	g.session = g.cluster.connect()

@app.after_request
def after_request(response):
	# Shutdown the session
	g.cluster.shutdown()
	return response

if __name__ == '__main__':
	app.debug = True
	app.run(host='0.0.0.0')
