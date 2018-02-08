package org.insight.fluid;

import java.util.Properties;
import info.debatty.java.stringsimilarity.Cosine;
import java.util.ArrayList;
import java.lang.Long;
import java.lang.Integer;
import java.lang.System;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.Iterator;


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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;

// Redis client libs
import redis.clients.jedis.Jedis;

// Local project libs
import org.myorg.quickstart.MessageObject;
import org.myorg.quickstart.PublisherJedisHandle;
import org.myorg.quickstart.JedisMessageWriter;
import org.myorg.quickstart.NaiveSimilarity;


public class MessageStreamProcessor {

	public final class JsonToMessageObjectMapper implements MapFunction<ObjectNode, MessageObject> {
		@Override
		public MessageObject map(ObjectNode obj) throws Exception {
			
			MessageObject outobj = new MessageObject();

			outobj.id = obj.has("id") ? obj.get("id").asText() : "";
			outobj.thread_id = obj.has("link_id") ? obj.get("link_id").asText() : "";
			outobj.author_id = obj.has("author_id") ? obj.get("author_id").asText() : "";
			outobj.score = Integer.valueOf(obj.has("score") ? obj.get("score").asText() : "0");
			outobj.parent_id = obj.has("parent_id") ? obj.get("parent_id").asText() : "";
			outobj.subreddit_id = obj.has("subreddit_id") ? obj.get("subreddit_id").asText() : "";
			outobj.body = obj.has("body") ? obj.get("body").asText() : "";

			/* Write messages to Redis cache and to the thread comments PubSub queue */
			JedisMessageWriter.getInstance().writeMessage(outobj.id, outobj.thread_id, outobj.parent_id, outobj.subreddit_id, outobj.author_id, outobj.body, outobj.score.toString());

			return outobj;
		}
	}

	final String ADS_TABLE_KEY_PREFIX = 	"AD_KEY_PREFIX_";
	final String ADS_TABLE = 				"REDIS_ADS_TABLE";


	/*
	 * 	Core logic for stream processing implemented here.
	 *	Responsibilities:
	 *	1. Stores all the ads in-memory as keyed stream state, read in from Redis
	 *	2. Takes a message object within a keyed stream (keyed on the conversation thread id)
	 *		and calculates a match score for all ads stored in-memory in the state. 
	 */
	public class MessageAdProcessorStateful extends RichMapFunction<MessageObject, Tuple2<String, String>> {

		private transient MapState<String, Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double>> adscorestate;

		private final int RUNNING_WINDOW_LEN = 8;		/* Length of window of last messages to consider for score computation */

		@Override
		public Tuple2<String, String> map(MessageObject in) throws Exception {
			
			String msg_thread_id = in.thread_id;
			String msg_text = in.body;

			String best_ad = "";
			Double best_score = new Double(-1);

			Iterator<Map.Entry<String, Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double>>> it = adscorestate.iterator();

			/* If ads are not loaded into memory yet, load them now from Redis */
			if (!it.hasNext()) {

				Jedis jedisAds = JedisAdsReader.getInstance().getHandle();	// Read ads from this 

				if (jedisAds.exists(ADS_TABLE)) {
					Long len = jedisAds.llen(ADS_TABLE);

					// Loop
					for (Long i = new Long(0); i < len; i++) {
						String ad_obj_key = jedisAds.lindex(ADS_TABLE, i.longValue());

						if (!ad_obj_key.startsWith(ADS_TABLE_KEY_PREFIX)) {
							System.out.println("AD object prefix is not valid!!!");
						}

						String ad_id = ad_obj_key.substring(ADS_TABLE_KEY_PREFIX.length());

						if (jedisAds.exists(ad_obj_key) && jedisAds.llen(ad_obj_key) == new Integer(3).longValue()) {
							String ad_title = jedisAds.lindex(ad_obj_key, new Integer(0).longValue());
							String ad_body = jedisAds.lindex(ad_obj_key, new Integer(1).longValue());
							String ad_tags = jedisAds.lindex(ad_obj_key, new Integer(2).longValue());

							Tuple3<String, String, String> ad_data = new Tuple3<>(ad_title, ad_body, ad_tags);

							ArrayList<Double> windowedScores = new ArrayList<Double>(RUNNING_WINDOW_LEN);
							for (int j = 0; j < RUNNING_WINDOW_LEN; j++) {
								windowedScores.add(new Double(0));
							}

							Double total = new Double(0);

							Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double> entry = new Tuple3<>(ad_data, windowedScores, total);

							adscorestate.put(ad_id, entry);
						}
					}
				}

				// Close to return to pool
				JedisAdsReader.getInstance().returnHandle(jedisAds);
			}

			it = adscorestate.iterator();
			
			while (it.hasNext()) {

				Map.Entry<String, Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double>> entry = it.next();
				String ad_id = entry.getKey();
				Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double> ad_data = entry.getValue();

				String ad_title = ad_data.f0.f0;
				String ad_body = ad_data.f0.f1;
				String ad_tags = ad_data.f0.f2;

				ArrayList<Double> prevScores = ad_data.f1;
				
				NaiveSimilarity ns = new NaiveSimilarity();
				Double similarity = ns.computeSimilarity(ad_body, msg_text);

				prevScores.add(similarity);
				Double evicted = prevScores.remove(0);

				Double totalWindowedScore = new Double(0);

				for (Integer i = 1; i <= RUNNING_WINDOW_LEN; i++) {
					totalWindowedScore += i.doubleValue() * prevScores.get(i.intValue()-1);
				}


				if (totalWindowedScore > best_score) {
					best_score = totalWindowedScore;
					best_ad = ad_id;
				}

				// Update state
				ad_data.f1 = prevScores;
				ad_data.f2 = totalWindowedScore;
				adscorestate.put(ad_id, ad_data);
			}

			return new Tuple2<>(msg_thread_id, best_ad);
		}

		@Override
		public void open(Configuration config) throws Exception {

	    		MapStateDescriptor<String, Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double>> descriptor =
        	    	new MapStateDescriptor<>(
                	    "adscorestate", 											// the state name
                    		TypeInformation.of(new TypeHint<String>() {}),
                    		TypeInformation.of(new TypeHint<Tuple3<Tuple3<String, String, String>, ArrayList<Double>, Double>>() {}) ); // type information
    			adscorestate = getRuntimeContext().getMapState(descriptor);
		}

	}

	/*
	 *	Publishes the ad match recommendations generated per conversation thread, into the match queue for that thread
	 */
	public class OutputToRedisPublisherMap implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {
		@Override
		public Tuple2<String, String> map(Tuple2<String, String> in) {

			PublisherJedisHandle.getInstance().publishMatch(in.f0, in.f1);

			return in;
		}
	}

	public static void main(String[] args) throws Exception {
		new MessageStreamProcessor().start();
	}

	final String KAFKA_TOPICNAME_FILE = "/home/ubuntu/Fluid/kafka/kafkatopicname";

	public void start() throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		File f = new File(KAFKA_TOPICNAME_FILE);
		if (!f.exists()) {
			System.out.println("Kafka topicname file does not exist!");
			System.exit(1);
		}

		FileReader fileReader = new FileReader(f);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String TOPICNAME = bufferedReader.readLine().trim();
		System.out.println("TOPICNAME is: " + TOPICNAME);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.0.10:9092,10.0.0.5:9092,10.0.0.11:9092");
        properties.setProperty("group.id", "consumergroup4");

		DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer09<>(TOPICNAME, new JSONDeserializationSchema(), properties));

		stream.map(new JsonToMessageObjectMapper())

			.keyBy("thread_id")

			.map(new MessageAdProcessorStateful())

			.map(new OutputToRedisPublisherMap());

		env.execute("Stream processor");

	}
}
