from kafka import KafkaProducer
import csv
import time
import json

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

with open('C:/Users/mrupv/bits/spa/Assignment2/paysim1/PS_20174392719_1491204439457_log.csv','rt') as file:
    data = list(csv.DictReader(file))
    for row in data:
        print(json.dumps(row))
        producer.send(topic='fraud_log',value=json.dumps(row))
        time.sleep(1)
