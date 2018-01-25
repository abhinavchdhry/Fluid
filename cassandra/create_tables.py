from cassandra.cluster import Cluster

cluster = Cluster(['10.0.0.6', '10.0.0.7'])
session = cluster.connect()

create_keyspace = "CREATE KEYSPACE IF NOT EXISTS FINAL WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor':2}"
session.execute(create_keyspace)

create_table = """CREATE TABLE IF NOT EXISTS FINAL.OUTPUT_TABLE (
	thread_id text PRIMARY KEY,
        matched_ad_id text)"""

session.execute(create_table)

create_comments_table = """CREATE TABLE IF NOT EXISTS FINAL.COMMENTS_TABLE (
	id text PRIMARY KEY,
	thread_id text,
	subreddit_id text,
	author_id text,
	parent_id text,
	body text,
	score text
	)"""

session.execute(create_comments_table)

create_ads_table = """CREATE TABLE IF NOT EXISTS FINAL.ADS_TABLE (
	id text PRIMARY KEY,
	title text,
	body text,
	tags text
	)"""

session.execute(create_ads_table)

# Clear the table if it exists
session.execute("""TRUNCATE FINAL.OUTPUT_TABLE""")
session.execute("""TRUNCATE FINAL.COMMENTS_TABLE""")
session.execute("""TRUNCATE FINAL.ADS_TABLE""")
