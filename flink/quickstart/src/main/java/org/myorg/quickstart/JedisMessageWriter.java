package org.myorg.quickstart;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisMessageWriter {
	private static Jedis jedis;
	private static JedisMessageWriter jedis_handle;
	private static JedisPool jedis_pool;

	private static final String MESSAGES_DB_NAME = "REDIS_MESSAGES_DB";

	private JedisMessageWriter() { }

	public synchronized static JedisMessageWriter getInstance() {
		if (jedis_handle == null) {
			jedis_handle = new JedisMessageWriter();
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(1024);
			jedis_pool = new JedisPool(poolConfig, "10.0.0.4");
//			jedis = new Jedis("10.0.0.4", 6379);
		}
		return jedis_handle;
	}

	public static Jedis getHandle() {
		return jedis_pool.getResource();
	}

	public static void returnHandle(Jedis jedis) {
		jedis_pool.returnResource(jedis);
	}

	public static String getMessagesDBName() {
		return MESSAGES_DB_NAME;
	}

	public static void writeMessage(String id, String link_id, String parent_id, String subreddit_id, String author_id, String body, String score) {
		Jedis j = getInstance().getHandle();
		
		// Create the object. NOTE: Objects are pushed in front. So order in list will be opposite of insertion order here
                if (!j.exists("MESSAGE_OBJ_" + id))
                {
                        j.lpush("MESSAGE_OBJ_" + id, score, body, author_id, subreddit_id, parent_id, link_id);
                }

                // Secondary index on thread_id
                j.sadd("THREAD_MSG_MAP_" + link_id, "MESSAGE_OBJ_" + id);

		j.publish("THREAD_CHANNEL_" + link_id, body);

		getInstance().returnHandle(j);
	}
}

