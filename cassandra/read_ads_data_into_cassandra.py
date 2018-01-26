from cassandra.cluster import Cluster
import json

cluster = Cluster()
session = cluster.connect()

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS FINAL WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor':3}"
session.execute(create_keyspace)

create_table = """CREATE TABLE IF NOT EXISTS FINAL.ADS_TABLE (
	id text PRIMARY KEY,
	title text,
	body text,
	tags text)"""

session.execute(create_table)

def clear_table(session):
	session.execute("TRUNCATE FINAL.ADS_TABLE")

prep = session.prepare("INSERT INTO FINAL.ADS_TABLE (id, title, body, tags) VALUES (?, ?, ?, ?)")

clear_table(session)

f = open('../data/ads/chicago.data', 'r')
count = 0
for line in f:
	j = json.loads(line)

	session.execute(
	prep,
	(j["id"], j["title"], j["body"], j["tags"])
	)

	count += 1

print("Loaded " + str(count) + "rows")
