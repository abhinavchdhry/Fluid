package org.myorg.quickstart;

import redis.clients.jedis.Jedis;

public class PublisherJedisHandle {
	private static Jedis publisherJedis;
	private static PublisherJedisHandle jedis_handle;

	private static final String MATCH_PREFIX = "MATCH_";
	private static final String OUTPUT_CHANNEL_PREFIX = "AD_CHANNEL_";

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

	public static void publishMatch(String thread_id, String matched_ad_id) {
		Jedis j = getInstance().getHandle();

		j.set("MATCH_" + thread_id, matched_ad_id);
		j.publish(OUTPUT_CHANNEL_PREFIX + thread_id, matched_ad_id);
	}
}
