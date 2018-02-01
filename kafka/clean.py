from kafka import KafkaConsumer as kc

c = kc("jsontest22", bootstrap_servers=["10.0.0.10", "10.0.0.5", "10.0.0.11"], group_id="consumergroup4")

for msg in c:
	print(msg)
