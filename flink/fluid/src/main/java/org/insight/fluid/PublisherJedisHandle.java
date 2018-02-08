package org.insight.fluid;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PublisherJedisHandle {
	private static PublisherJedisHandle jedis_handle;
	private static JedisPool jedis_pool;

	private static final String MATCH_PREFIX = "MATCH_";
	private static final String OUTPUT_CHANNEL_PREFIX = "AD_CHANNEL_";

	private PublisherJedisHandle() { }

	public synchronized static PublisherJedisHandle getInstance() {
		if (jedis_handle == null) {
			jedis_handle = new PublisherJedisHandle();
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1024);
            jedis_pool = new JedisPool(poolConfig, "10.0.0.4");
		}
		return jedis_handle;
	}

	public static Jedis getHandle() {
		return jedis_pool.getResource();
	}

	public static void returnHandle(Jedis jedis) {
		jedis_pool.returnResource(jedis);
	}

	public static void publishMatch(String thread_id, String matched_ad_id) {
		Jedis j = getInstance().getHandle();

		j.set("MATCH_" + thread_id, matched_ad_id);
		j.publish(OUTPUT_CHANNEL_PREFIX + thread_id, matched_ad_id);

		getInstance().returnHandle(j);
	}
}
