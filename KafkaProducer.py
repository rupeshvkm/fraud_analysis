from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

def publish(message,topic):
        producer.send(topic=topic,value=message.encode('utf-8'))

