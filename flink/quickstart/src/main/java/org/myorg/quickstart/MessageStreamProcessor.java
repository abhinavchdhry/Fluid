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

public class MessageStreamProcessor {

	final static String TOPICNAME = "testtopic";

	public final class JsonToMessageObjectMapper implements MapFunction<ObjectNode, MessageObject> {
		@Override
		public MessageObject map(ObjectNode obj) throws Exception {
			return new MessageObject(
				obj.has("id") ? obj.get("id").asText() : "",
				obj.has("thread_id") ? obj.get("thread_id").asText() : "",
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

	public class MessageProcessor extends ProcessFunction<MessageObject, MessageObject> {
		@Override
    		public void processElement(MessageObject obj, Context ctx, Collector<MessageObject> out)
            		throws Exception 
		{
			String thread_id = obj.thread_id;

			DataSet<Tuple3<String, String, String>> obj_dataset = obj.toDataSet();
			DataSet<Tuple4<String, String, String, String>> adsData = CassandraSession.getInstance().getData();

			adsData.crossWithTiny(obj_dataset).map(

				new MapFunction<Tuple2<Tuple4<String, String, String, String>, Tuple3<String, String, String>>, Tuple3<String, String, Double>>() {
					@Override
					public Tuple3<String, String, Double> map(Tuple2<Tuple4<String, String, String, String>, Tuple3<String, String, String>> in) throws Exception {
						String ad_id = in.f0.f0;
						String ad_title = in.f0.f1;
						String ad_body = in.f0.f2;
						String ad_tags = in.f0.f3;

						String comment_thread_id = in.f1.f1;
						String comment_text = in.f1.f2;

						Cosine cosine = new Cosine(1);
						Double similarity = cosine.similarity(ad_title + ad_body + ad_tags, comment_text);
						Double overall_ad_similarity = new Double(-1.0);

						Jedis jedis = JedisHandle.getInstance().getHandle();

						// Retrieve context and push updated context to Jedis
						if (jedis.exists("THREAD_MAP") && jedis.hexists("THREAD_MAP", comment_thread_id)) {
							String cur_thread_map = jedis.hget("THREAD_MAP", comment_thread_id);

							if (jedis.exists(cur_thread_map)) {
								if (jedis.hexists(cur_thread_map, ad_id)) {
									String thread_ad_mapped_obj = jedis.hget(cur_thread_map, ad_id);

									if (jedis.exists(thread_ad_mapped_obj)) {
										if (jedis.hexists(thread_ad_mapped_obj, "Total_score") && jedis.hexists(thread_ad_mapped_obj, "Count")) {
											Double score = Double.parseDouble(jedis.hget(thread_ad_mapped_obj, "Total_score"));
											Integer count = Integer.parseInt(jedis.hget(thread_ad_mapped_obj, "Count"));

											count += 1;
											score += similarity;

											overall_ad_similarity = score/count.doubleValue();

											jedis.hset(thread_ad_mapped_obj, "Total_score", score.toString());
											jedis.hset(thread_ad_mapped_obj, "Count", count.toString());
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
		                                                        String object_name = comment_thread_id + "_" + ad_id + "_object";

                		                                        jedis.hset(object_name, "Total_score", similarity.toString());
                                		                        jedis.hset(object_name, "Count", "1");
									
									// Insert the thread_map object
									jedis.hset(cur_thread_map, ad_id, object_name);
								}
                                                        }
   	                                                else {	// Non null ID present but object does not exist!!
								System.out.println("JEDIS_OBJECT_NOT_FOUND1: THREAD_MAP referenced a non-null object but the object was not found!");
                                                        }
						}
						else {	// Create a THREAD_MAP, if exists also create the thread id map
							
							// Create the thread-ad map object
							String object_name = comment_thread_id + "_" + ad_id + "_object";

							jedis.hset(object_name, "Total_score", similarity.toString());
							jedis.hset(object_name, "Count", "1");

							// Create the thread map object
							String thread_map_object = comment_thread_id + "_object";
				
							jedis.hset(thread_map_object, ad_id, object_name);

							// Create the final THREAD_MAP hashmap
							jedis.hset("THREAD_MAP", comment_thread_id, thread_map_object);
						}

						return new Tuple3<>(comment_thread_id, ad_id, overall_ad_similarity);
					}
				}
			
			).maxBy(2).writeAsText("~/flink_output.txt", WriteMode.OVERWRITE).setParallelism(1);

		}
	}

	public static void main(String[] args) throws Exception {
		new MessageStreamProcessor().start();
	}

	public void start() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		FlinkKafkaConsumer09[] consumers = getConsumerGroup(3, "consumergroup1");
                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "localhost:9092");
                properties.setProperty("zookeeper.connect", "localhost:2181");
                properties.setProperty("group.id", "consumergroup1");

//		FlinkKafkaConsumer09<ObjectNode> consumer = new FlinkKafkaConsumer09<>(TOPICNAME, new JSONKeyValueDeserializationSchema(false), properties);

		DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer09<>("jsontest21", new JSONDeserializationSchema(), properties));
		stream.map(new JsonToMessageObjectMapper())
			.keyBy("thread_id")
			.process(new MessageProcessor());
//			.reduce(new ReduceToWords())
//			.writeAsText("/home/ubuntu/abhinav_words.txt", WriteMode.OVERWRITE)
//			.setParallelism(1);

		// versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
		// this method is not available for earlier Kafka versions
//		myProducer.setWriteTimestampToKafka(true);

//		stream.addSink(myProducer);


		env.execute("Stream processor");

	}
}
