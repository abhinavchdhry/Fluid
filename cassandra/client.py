from cassandra.cluster import Cluster
import json

cluster = Cluster()
session = cluster.connect()

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS ADS_KEYSPACE WITH REPLICATION = {'class' : 'NetworkTopologyStrategy'}"
session.execute(create_keyspace)

create_table = """CREATE TABLE IF NOT EXISTS ADS_KEYSPACE.ads_table (
	id text PRIMARY KEY,
	title text,
	body text,
	tags text)"""

session.execute(create_table)

def clear_table(session):
	session.execute("TRUNCATE ADS_KEYSPACE.ads_table")

insert_statement_body = "INSERT INTO ADS_KEYSPACE.ads_table (id, title, body, tags) VALUES "

clear_table(session)

f = open('../data/ads/chicago.data', 'r')
count = 0
for line in f:
	j = json.loads(line)

	session.execute(
	"""
	INSERT INTO ADS_KEYSPACE.ads_table (id, title, body, tags)
	VALUES (%s, %s, %s, %s)
	""",
	(j["id"], j["title"], j["body"], j["tags"])
	)

	count += 1
	if count == 10:
		break
