#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaProducer
topic_name = 'items'
kafka_server = 'localhost:9092'
producer = KafkaProducer (bootstrap_servers=kafka_server)
producer.send(topic_name, b'Hello Kafka Python sadsadsad!!!')
producer.flush()


# In[ ]:


from kafka import KafkaProducer
from json import dumps
from time import sleep

topic_name = 'RandomNumber'
kafka_server = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda x: dumps(x).encode('utf-8'))

for e in range(1000):
    data = {'number': e}
    producer.send(topic_name, value=data)
    print(str(data) + " sent")
    sleep(5)

producer.flush()


# In[ ]:


topic_name = 'RandomNumber'
kafka_server = 'localhost:9092'

# Define the Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_server,
    "subscribe": topic_name,
    "startingOffsets": "earliest"
}

# Read data from Kafka
kafka_df = spark.read.format("kafka").options(**kafka_options).load()
kafka_df.toPandas()


# In[ ]:




