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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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

// Redis client libs
import redis.clients.jedis.Jedis;

// Local project libs
import org.myorg.quickstart.MessageObject;
import org.myorg.quickstart.JedisHandle;
import org.myorg.quickstart.CassandraSession;

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
			Jedis jedis = JedisHandle.getInstance().getHandle();

			DataSet<Tuple2<String, String>> obj_dataset = obj.toDataSet();
			DataSet<Tuple4<String, String, String, String>> adsData = CassandraSession.getInstance().getData();

//			adsData.crossWithTiny(obj_dataset);

/*			if (jedis.hexists("THREAD_HASHMAP", thread_id)) {
				String currentThreadTableKey = jedis.hget("THREAD_HASHMAP", thread_id);
				
			}
			else {
				System.out.println("THREAD_HASHMAP does not exist!");
			}
*/
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
