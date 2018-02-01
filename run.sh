# Clear current kafka topic
topic=$1
if [ -z $1 ]
then
	echo "Argument empty: Needs a topicname"
	exit 1
fi

echo "Topic name is: " $1

echo $topic > /home/ubuntu/Fluid/kafka/kafkatopicname

#echo "Clearing Kafka topic:" $topic
#ssh ubuntu@10.0.0.10 "/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic " $topic " --config retention.ms=1"
#echo "Retention.ms set to 1. Verifying..."
#ssh ubuntu@10.0.0.10 "/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic " $topic

#echo "Waiting 2 minutes for topic flush to take effect..."
#sleep 2m

#echo "Resetting retention.ms..."
#ssh ubuntu@10.0.0.10 "/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic " $topic " --config retention.ms=86400000"

echo "Starting kafka producer..."
python ~/Fluid/kafka/producer.py &
echo "Done"

# Clean Redis data
echo "Flushing Redis tables..."
python ~/Fluid/redis/clear_cached_redis_tables.py
echo "Done"
echo "Loading 10 ads..."
python ~/Fluid/redis/load_ads_data_to_redis.py 10
echo "Done"

echo "Building Flink jar..."
mvn install -Pbuild-jar -f /home/ubuntu/Fluid/flink/quickstart/pom.xml

echo "Running Flink job..."
/usr/local/flink/bin/flink run -c org.myorg.quickstart.MessageStreamProcessor ~/Fluid/flink/quickstart/target/quickstart-0.1.jar &

wait
