from pykafka import KafkaClient
#initialize kafka client
client = KafkaClient(hosts="127.0.0.1:9092")
#get topic from kafka client
topic = client.topics['Tweets']
consumer = topic.get_simple_consumer()
while True:
	for message in consumer:
	    if message is not None:
	    	print(message.offset, message.value)