import json
import os
from kafka import KafkaProducer

### Takes a filepath and a KafkaProducer object and sends
### all data from files in <filepath> to the Kafka brokers
### customKeyFunc(record) is a user-defined function that
### generates a key for every record sent to the Kafka brokers
### The function is called with the current record read from file

class Generator(object):
	def __init__(self, filepath, producer, topic, customKeyFunc=None):
		self._filepath = filepath
		self._producer = producer
		self._topic = topic
		self._keyFunc = customKeyFunc

	def _processFile(self, filepath):
		try:
			fh = open(filepath,'r')
		except:
			print("Error opening file: " + filepath + "!")
			return

		if self._keyFunc:
			for line in fh:
				self._producer.send(self._topic, value=line, key=self._keyFunc(line))
		else:
			for line in fh:
				self._producer.send(self._topic, value=line)

	def start(self):
		files = os.listdir(self._filepath)
		for _f in files:
			self._processFile(self._filepath + filename)


PATH = '../data/'
TOPICNAME = 'COMMENTS'

def customPartitionFunc(key_bytes, all_partitions, available_partitions):
	n_partitions = len(available_partitions)
	try:
		k = int(key_bytes)
	exept ValueError:
		print('WARNING (' + customPartitionFunc.__name__ +  '): could not convert key to int. Defaulting to 0')
		return 0

	return k%n_partitions

CONFIGS = {
	'bootstrap_servers': ,
	'partitioner': customPartitionFunc
}

producer = KafkaProducer(**CONFIGS)

## Creates an integer key for every record parsed
def customKeyFunc(record):
	deserialized = json.loads(record)
	if 'link_id' in deserialized:
		k = deserialized['link_id'][-1]
		k = ord(k) - ord('a')
	else:
		k = 0		# Default to 0
	
	return k

## Main
g = Generator(PATH, producer, TOPICNAME, customKeyFunc)
g.start()
