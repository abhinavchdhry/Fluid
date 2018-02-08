from flask import Flask
from flask import render_template
from flask import Response
import time
import redis
import json
from random import sample

app = Flask(__name__)


## Setup connection to Redis
r = redis.StrictRedis(host='10.0.0.4', db=0)


@app.route('/')
def entry():

	curMatches = r.keys("MATCH_*")

	if len(curMatches) > 25:
		curMatches = sample(curMatches, 25)

	out = {}
	for match in curMatches:
		thread_id = match[len('MATCH_'):]
		msg_obj = r.lindex("THREAD_MSG_MAP_" + thread_id, 0)
	
		body = r.lindex(msg_obj, 4).decode('utf-8')
		if '[removed]' not in body and '[deleted]' not in body:
			out[thread_id] = body
	
	### FOR DEMO: fixed thread
	out = {}
	fixed_id = 't3_sample'
	fixed_obj = {'id':fixed_id, 'body':'Click here to view demo conversation'}

	return render_template('index.html', result=out, fixed=fixed_obj)

@app.route('/<thread_id>')
def show_thread(thread_id):
	body = {}
	numMessagesInThread = r.scard("THREAD_MSG_MAP_" + thread_id)

	# Bug fix: Load only 100 messages for this thread when numMessagesInThread exceeds 100
	# otherwise page does not load
	numMessagesInThread = min(numMessagesInThread, 100)

	messages = r.srandmember("THREAD_MSG_MAP_" + thread_id, numMessagesInThread)

	for i in range(numMessagesInThread):
		msg_key = messages[i]
		msg_id = msg_key[12:]
		msg_author = r.lindex(msg_key, 3).decode('utf-8')
		msg_author = 'Anonymous' if msg_author.strip() == '' else msg_author.title()
		msg_body = r.lindex(msg_key, 4).decode('utf-8')
		body[msg_id] = { 'body': msg_body, 'author': msg_author }
	
	# Get the currently matched ad now
	matched_ad_id = r.get("MATCH_" + thread_id)
	matched_ad = {}
	matched_ad['title'] = r.lindex("AD_KEY_PREFIX_" + matched_ad_id, 0).decode('utf-8')
	matched_ad['body'] = r.lindex("AD_KEY_PREFIX_" + matched_ad_id, 1).decode('utf-8')
	matched_ad['tags'] = r.lindex("AD_KEY_PREFIX_" + matched_ad_id, 2).decode('utf-8')

	if r.llen("AD_KEY_PREFIX_" + matched_ad_id) == 4:
		matched_ad['img'] = r.lindex("AD_KEY_PREFIX_" + matched_ad_id, 3)

	return render_template('thread.html', thread_id=thread_id, results=body, matched_ad=matched_ad)


def comment_stream_source(thread_id):
	subscriber = r.pubsub()
	subscribe_channel = "THREAD_CHANNEL_" + thread_id
	print("Comment stream: Subscribing to: ", subscribe_channel)
	subscriber.subscribe(subscribe_channel)
	for msg in subscriber.listen():
		if msg['type'] == 'message':
			yield "data: %s\n\n" % msg['data']


@app.route('/comment-stream/<thread_id>')
def comment_stream(thread_id):
	print('Comment stream started...')
	return Response(comment_stream_source(thread_id), mimetype="text/event-stream")


def ad_stream_source(thread_id):
	ad_subscriber = r.pubsub()
	ad_subscribe_channel = "AD_CHANNEL_" + thread_id
	print("Ad stream: Subscribing to: ", ad_subscribe_channel)
        ad_subscriber.subscribe(ad_subscribe_channel)
	last_msg_time = None
	for msg in ad_subscriber.listen():
		if last_msg_time is None or (time.time() - last_msg_time) > 2:
			last_msg_time = time.time()
		else:
			continue
		if msg['type'] == 'message':
			matched_ad_id = msg['data']
			print("Matched ad: " + matched_ad_id)
			title = r.lindex('AD_KEY_PREFIX_' + matched_ad_id, 0)
			body = r.lindex('AD_KEY_PREFIX_' + matched_ad_id, 1)
			img = r.lindex('AD_KEY_PREFIX_' + matched_ad_id, 3) if r.llen('AD_KEY_PREFIX_' + matched_ad_id) == 4 else None
			
			if img is None:
				out = "data: {\"title\":\"" + title + "\", \"body\":\"" + body + "\"}\n\n"
			else:
				out = "data: {\"title\":\"" + title + "\", \"body\":\"" + body + "\", \"img\":\"" + img + "\"}\n\n"
			yield out


@app.route('/ad-stream/<thread_id>')
def ad_stream(thread_id):
	print("Ad stream started...")
	return Response(ad_stream_source(thread_id), mimetype="text/event-stream")

if __name__ == '__main__':
	app.debug = True
	app.run(host='0.0.0.0', threaded=True)
