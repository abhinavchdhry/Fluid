package org.myorg.quickstart;

import redis.clients.jedis.Jedis;

public class PublisherJedisHandle {
	private static Jedis publisherJedis;
	private static PublisherJedisHandle jedis_handle;

	private PublisherJedisHandle() { }

	public static PublisherJedisHandle getInstance() {
		if (publisherJedis == null) {
			jedis_handle = new PublisherJedisHandle();
			publisherJedis = new Jedis("10.0.0.4", 6379);
		}
		return jedis_handle;
	}

	public static Jedis getHandle() {
		return publisherJedis;
	}
}
