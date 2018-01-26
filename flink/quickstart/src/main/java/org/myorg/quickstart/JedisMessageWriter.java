package org.myorg.quickstart;

import redis.clients.jedis.Jedis;

public class JedisMessageWriter {
	private static Jedis jedis;
	private static JedisMessageWriter jedis_handle;

	private static final String MESSAGES_DB_NAME = "REDIS_MESSAGES_DB";

	private JedisMessageWriter() { }

	public static JedisMessageWriter getInstance() {
		if (jedis == null) {
			jedis_handle = new JedisMessageWriter();
			jedis = new Jedis("10.0.0.4", 6379);
		}
		return jedis_handle;
	}

	public static Jedis getHandle() {
		return jedis;
	}

	public static String getMessagesDBName() {
		return MESSAGES_DB_NAME;
	}

	public static void writeMessage(String id, String link_id, String parent_id, String subreddit_id, String author_id, String body, String score) {
		Jedis j = getInstance().getHandle();
		
		// Create the object. NOTE: Objects are pushed in front. So order in list will be opposite of insertion order here
		j.lpush("MESSAGE_OBJ_" + id, score, body, author_id, subreddit_id, parent_id, link_id);

		// Secondary index on thread_id
		j.lpush("THREAD_MSG_MAP_" + link_id, "MESSAGE_OBJ_" + id);

		j.publish("THREAD_CHANNEL_" + link_id, body);
	}
}
