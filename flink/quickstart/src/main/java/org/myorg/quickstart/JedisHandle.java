package org.myorg.quickstart;

import redis.clients.jedis.Jedis;

public class JedisHandle {
	private static Jedis jedis;
	private static JedisHandle jedis_handle;

	private JedisHandle() { }

	public static JedisHandle getInstance() {
		if (jedis == null) {
			jedis_handle = new JedisHandle();
			jedis = new Jedis("localhost");
		}
		return jedis_handle;
	}

	public static Jedis getHandle() {
		return jedis;
	}
}
