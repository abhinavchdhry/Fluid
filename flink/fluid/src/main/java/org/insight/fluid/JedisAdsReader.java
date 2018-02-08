package org.insight.fluid;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisAdsReader {
	private static Jedis jedis;
	private static JedisAdsReader jedis_handle;
	private static JedisPool jedis_pool;

	private JedisAdsReader() { }

        public synchronized static JedisAdsReader getInstance() {

            if (jedis_handle == null) {
                jedis_handle = new JedisAdsReader();
				JedisPoolConfig poolConfig = new JedisPoolConfig();
				poolConfig.setMaxTotal(1024);
				jedis_pool = new JedisPool(poolConfig, "10.0.0.4");
			
				if (jedis_pool == null) {
					System.out.println("Failed to initialize jedis_pool!");
				}
            }

            return jedis_handle;
        }

	public static Jedis getHandle() {
		return jedis_pool.getResource();
	}

	public static void returnHandle(Jedis jedis) {
		jedis_pool.returnResource(jedis);
	}
}
