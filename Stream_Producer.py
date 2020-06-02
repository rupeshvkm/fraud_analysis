from kafka import KafkaProducer as kp
import csv
import time
import json

producer = kp(bootstrap_servers = 'localhost:9092')

with open('C:/Users/mrupv/bits/spa/Assignment2/paysim1/train.csv','rt') as file:
    data = list(csv.DictReader(file))
    for row in data:
        print(json.dumps(row))
        producer.send(topic='fraud_log',value=json.dumps(row).encode('utf-8'))
        time.sleep(3)
