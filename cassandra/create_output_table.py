from cassandra.cluster import Cluster

cluster = Cluster(['10.0.0.6', '10.0.0.7'])
session = cluster.connect()

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS FINAL WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor':2}"
session.execute(create_keyspace)

create_table = """CREATE TABLE IF NOT EXISTS FINAL.OUTPUT_TABLE (
	thread_id text PRIMARY KEY,
        matched_ad_id text)"""

session.execute(create_table)

