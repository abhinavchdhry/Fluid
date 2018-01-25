from cassandra.cluster import Cluster

cluster = Cluster(['10.0.0.6', '10.0.0.7'])
session = cluster.connect()

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS FINAL WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor':2}"
session.execute(create_keyspace)

create_table = """CREATE TABLE IF NOT EXISTS FINAL.OUTPUT_TABLE (
	thread_id text PRIMARY KEY,
        matched_ad_id text)"""

session.execute(create_table)

# Clear the table if it exists
session.execute("""TRUNCATE FINAL.OUTPUT_TABLE""")

create_msgs_table = """CREATE TABLE IF NOT EXISTS FINAL.MESSAGES (
	id text PRIMARY KEY,
	thread_id text,
	author_id text,
	parent_id text,
	subreddit_id text,
	body text,
	score text
	)"""

session.execute(create_msgs_table)

session.execute("""TRUNCATE FINAL.MESSAGES""")

## Create a secondary index on the Messages table on thread_id column
session.execute("""CREATE INDEX thread_id_index ON FINAL.MESSAGES (thread_id)""")
