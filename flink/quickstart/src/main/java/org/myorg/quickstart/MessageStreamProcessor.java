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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.myorg.quickstart.MessageObject;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;

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
//		stream.map(new MyMapFunction()).map(new MessageObjToStringMap()).writeAsText("/home/ubuntu/abhinav1.txt").setParallelism(1);
		stream.map(new JsonToMessageObjectMapper())
			.keyBy("thread_id")
			.reduce(new ReduceToWords())
			.writeAsText("/home/ubuntu/abhinav_words.txt", WriteMode.OVERWRITE)
			.setParallelism(1);

		Cluster cluster = null;
		try {
			cluster = Cluster.builder()                                                    // (1)
            		.addContactPoint("localhost")
            			.build();
			if (cluster == null) {
				System.out.println("Cluster is null");
			}
			Session session = cluster.connect();                                           // (2)
			ResultSet rs = session.execute("select release_version from system.local");    // (3)
			Row row = rs.one();
			System.out.println(row.getString("release_version"));                          // (4)
		} catch (Exception e) {
			System.out.println("OOPS!");
		} finally {
			if (cluster != null) cluster.close();                                          // (5)
		}

		// versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
		// this method is not available for earlier Kafka versions
//		myProducer.setWriteTimestampToKafka(true);

//		stream.addSink(myProducer);


		env.execute("Stream processor");

	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
