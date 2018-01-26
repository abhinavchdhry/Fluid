package org.myorg.quickstart;

import redis.clients.jedis.Jedis;

public class JedisAdsReader {
	private static Jedis jedis;
	private static JedisAdsReader jedis_handle;

	private JedisAdsReader() { }

	public static JedisAdsReader getInstance() {
		if (jedis == null) {
			jedis_handle = new JedisAdsReader();
			jedis = new Jedis("10.0.0.4", 6379);
		}
		return jedis_handle;
	}

	public static Jedis getHandle() {
		return jedis;
	}
}
