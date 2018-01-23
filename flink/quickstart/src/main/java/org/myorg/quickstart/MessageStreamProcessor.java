package org.myorg.quickstart;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties;

// FLink libs
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// Cassandra driver libs
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;

import com.datastax.driver.core.Cluster.Builder;

import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;


// Redis client libs
import redis.clients.jedis.Jedis;

// Local project libs
import org.myorg.quickstart.MessageObject;
import org.myorg.quickstart.JedisHandle;
import org.myorg.quickstart.CassandraSession;

import info.debatty.java.stringsimilarity.Cosine;
import java.util.ArrayList;
import java.lang.Long;
import java.lang.Integer;

public class MessageStreamProcessor {

	final static String TOPICNAME = "testtopic";

	public final class JsonToMessageObjectMapper implements MapFunction<ObjectNode, MessageObject> {
		@Override
		public MessageObject map(ObjectNode obj) throws Exception {
			return new MessageObject(
				obj.has("id") ? obj.get("id").asText() : "",
				obj.has("link_id") ? obj.get("link_id").asText() : "",
				obj.has("author_id") ? obj.get("author_id").asText() : "",
				obj.has("score") ? obj.get("score").asInt() : 0,
				obj.has("parent_id") ? obj.get("parent_id").asText() : "",
				obj.has("subreddit_id") ? obj.get("subreddit_id").asText() : "",
				obj.has("body") ? obj.get("body").asText() : ""
			);
		}
	}

	public final class MessageObjToStringMap implements MapFunction<MessageObject, String> {
		@Override
		public String map(MessageObject obj) throws Exception {
			return obj.toString();
		}
	}

	public final class ReduceToWords implements ReduceFunction<MessageObject> {
		@Override
		public MessageObject reduce(MessageObject o1, MessageObject o2) throws Exception {
			return new MessageObject(
				o1.id,
				o1.thread_id,
				o1.author_id,
				o1.score + o2.score,
				o1.parent_id,
				o1.subreddit_id,
				o1.body + o2.body
			);
		}
	}

	private static FlinkKafkaConsumer09[] getConsumerGroup(Integer numConsumers, String groupId) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", groupId);

		FlinkKafkaConsumer09[] consumers = new FlinkKafkaConsumer09[numConsumers];
		for (int i = 0; i < numConsumers; i++)
		{
			consumers[i] = new FlinkKafkaConsumer09<>(TOPICNAME, new JSONDeserializationSchema(), properties);
		}

		return (consumers);
	}

	final String ADS_TABLE_KEY_PREFIX = "AD_KEY_PREFIX_";
	final String ADS_TABLE = "REDIS_ADS_TABLE";

	final String THREAD_MAP_OBJ = "THREAD_MAP";
	final String THREAD_KEY_PREFIX = "THREAD_ID_KEY_";
	final String THREAD_VAL_PREFIX = "THREAD_VAL_KEY_";
	final String AD_KEY_PREFIX = "AD_ID_KEY_";
	final String SCORE_FIELD = "TOTAL_SCORE";
	final String COUNT_FIELD = "TOTAL_COUNT";
	final String CACHED_OBJ_PREFIX = "CACHED_OBJ_";

	/*
		Output tuples have format:
		1	Comment ID
		2	Thread ID
		3	Comment Body
		4	Ad ID
		5	Ad title
		6	Ad body
		7	Ad tags
	*/
	public class MessageAdProcessor implements MapFunction<Tuple7<String, String, String, String, String, String, String>, Tuple3<String, String, Double>> {
		@Override
		public Tuple3<String, String, Double> map(Tuple7<String, String, String, String, String, String, String> in) throws Exception {
			Jedis jedis = JedisHandle.getInstance().getHandle();

			Double highest_score = new Double(-1);
			String best_ad_id = "";

			if (jedis.exists(ADS_TABLE)) {
				Long len = jedis.llen(ADS_TABLE);

				// Loop
				for (Long i = new Long(0); i < len; i++) {
					String ad_obj_key = jedis.lindex(ADS_TABLE, i);

					if (!ad_obj_key.startsWith(ADS_TABLE_KEY_PREFIX)) {
						System.out.println("AD object prefix is not valid!!!");
					}

					String ad_id = ad_obj_key.substring(ADS_TABLE_KEY_PREFIX.length()-1);

					if (jedis.exists(ad_obj_key) && jedis.llen(ad_obj_key) == new Integer(3).longValue()) {
						String ad_title = jedis.lindex(ad_obj_key, new Integer(0).longValue());
						String ad_body = jedis.lindex(ad_obj_key, new Integer(1).longValue());
						String ad_tags = jedis.lindex(ad_obj_key, new Integer(2).longValue());

						String comment_id = in.f0;
						String comment_thread_id = in.f1;
						String comment_text = in.f2;

						Cosine cosine = new Cosine(1);
						Double similarity = cosine.similarity(ad_title + ad_body + ad_tags, comment_text);
						Double overall_ad_similarity = similarity;

						// Retrieve context and push updated context to Jedis
						if (jedis.exists(THREAD_MAP_OBJ) && jedis.hexists(THREAD_MAP_OBJ, THREAD_KEY_PREFIX + comment_thread_id)) {

							String cur_thread_map = jedis.hget(THREAD_MAP_OBJ, THREAD_KEY_PREFIX + comment_thread_id);

							if (jedis.exists(cur_thread_map)) {
								if (jedis.hexists(cur_thread_map, AD_KEY_PREFIX + ad_id)) {
									String thread_ad_mapped_obj = jedis.hget(cur_thread_map, AD_KEY_PREFIX + ad_id);

									if (jedis.exists(thread_ad_mapped_obj)) {
										if (jedis.hexists(thread_ad_mapped_obj, SCORE_FIELD) && jedis.hexists(thread_ad_mapped_obj, COUNT_FIELD)) {
											Double score = Double.parseDouble(jedis.hget(thread_ad_mapped_obj, SCORE_FIELD));
											Integer count = Integer.parseInt(jedis.hget(thread_ad_mapped_obj, COUNT_FIELD));

											count += 1;
											score += similarity;

											overall_ad_similarity = score/count.doubleValue();

											jedis.hset(thread_ad_mapped_obj, SCORE_FIELD, score.toString());
											jedis.hset(thread_ad_mapped_obj, COUNT_FIELD, count.toString());
										}
										else {
											System.out.println("JEDIS_OBJECT_CORRUPT1: Corrupt thread_ad_mapped_obj: either total_score or count field missing!");
										}
									}
									else {
										System.out.println("JEDIS_OBJECT_NOT_FOUND2: thread_ad_mapped_obj not found!");
									}
								}
								else {	// Current thread does not contain ad_id. Make an entry
									// Create the thread-ad map object
						                        String object_name = CACHED_OBJ_PREFIX + comment_thread_id + "_" + ad_id;

							                jedis.hset(object_name, SCORE_FIELD, similarity.toString());
								        jedis.hset(object_name, COUNT_FIELD, "1");
						
									// Insert the thread_map object
									jedis.hset(cur_thread_map, AD_KEY_PREFIX + ad_id, object_name);
								}
						        }
						        else {	// Non null ID present but object does not exist!!
								System.out.println("JEDIS_OBJECT_NOT_FOUND1: THREAD_MAP referenced a non-null object but the object was not found!");
						        }
						}
						else {	// Create a THREAD_MAP, if exists also create the thread id map
				
							// Create the thread-ad map object
							String object_name = CACHED_OBJ_PREFIX + comment_thread_id + "_" + ad_id;

							jedis.hset(object_name, SCORE_FIELD, similarity.toString());
							jedis.hset(object_name, COUNT_FIELD, "1");

							// Create the thread map object
							String thread_map_object = THREAD_VAL_PREFIX + comment_thread_id;
	
							jedis.hset(thread_map_object, AD_KEY_PREFIX + ad_id, object_name);

							// Create the final THREAD_MAP hashmap
							jedis.hset(THREAD_MAP_OBJ, THREAD_KEY_PREFIX + comment_thread_id, thread_map_object);
						}

						if (overall_ad_similarity > highest_score) {
							highest_score = overall_ad_similarity;
							best_ad_id = ad_id;
						}

					}
					else {
						System.out.println("Ad Object referenced does not exist or is not of appropriate length!!");
					}
				}
			}
			else {
				System.out.println("REDIS_ADS_TABLE not found!!");
			}

			return new Tuple3<String, String, Double>(in.f1, best_ad_id, highest_score);
		}
	}

	public class MessageToDummyTuple7Map implements MapFunction<MessageObject, Tuple7<String, String, String, String, String, String, String>> {
		@Override
		public Tuple7<String, String, String, String, String, String, String> map(MessageObject obj) {
			return new Tuple7<>(obj.id, obj.thread_id, obj.body, "", "", "", "");
		}
	}


	public static void main(String[] args) throws Exception {
		new MessageStreamProcessor().start();
	}

	public void start() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "10.0.0.10:9092,10.0.0.5:9092,10.0.0.11:9092");
//                properties.setProperty("zookeeper.connect", "10.0.0.10:2181,10.0.0.5:2181,10.0.0.11:2181");
                properties.setProperty("group.id", "consumergroup4");

//		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer09<>("jsontest21", new SimpleStringSchema(), properties));
//		stream.writeAsText("~/whatta.txt", WriteMode.OVERWRITE);
		
		DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer09<>("jsontest21", new JSONDeserializationSchema(), properties)).setParallelism(4);
		stream.map(new JsonToMessageObjectMapper()).setParallelism(4)
			.keyBy("thread_id")
			.map(new MessageToDummyTuple7Map()).setParallelism(4)	//.writeAsText("~/idunnowhat.txt", WriteMode.OVERWRITE).setParallelism(1);
//			.map(new MessageAdProcessor())
			.writeAsText("~/idunnowhat.txt", WriteMode.OVERWRITE).setParallelism(8);

		// versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
		// this method is not available for earlier Kafka versions
//		myProducer.setWriteTimestampToKafka(true);

		env.execute("Stream processor");

	}
}
