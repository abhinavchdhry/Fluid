# Clear current kafka topic
topic=$1
if [ $# -ne 2 ]
then
	echo "Needs 2 arguments: topicname and Flink job parallelism"
	exit 1
fi

echo "Topic name is: " $1

echo $topic > /home/ubuntu/Fluid/kafka/kafkatopicname

# Clean Redis data
echo "Flushing Redis tables..."
python ~/Fluid/redis/clear_cached_redis_tables.py
echo "Done"
echo "Loading 10 ads..."
python ~/Fluid/redis/load_ads_data_to_redis.py 10
echo "Done"

echo "Starting kafka producer..."
python ~/Fluid/kafka/producer.py &
echo "Done"

echo "Running Flink job..."
/usr/local/flink/bin/flink run -p $2 -c org.myorg.quickstart.MessageStreamProcessor ~/Fluid/flink/quickstart/target/quickstart-0.1.jar &

wait
