from kafka import KafkaProducer
import csv
import time
import json
from json import dumps

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

with open('C:/Users/mrupv/bits/spa/Assignment2/paysim1/train.csv','rt') as file:
    data = list(csv.DictReader(file))
    for row in data:
        print(json.dumps(row))
        producer.send(topic='fraud.logs',value=row)
        time.sleep(3)
