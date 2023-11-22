#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
topic_name = 'RandomNumber'
kafka_server = 'localhost:9092'
consumer = KafkaConsumer(topic_name)
for msg in consumer:
    print (msg)


# In[1]:


import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

scala_version = '2.12'  
spark_version = '3.0.1'  
kafka_version = '2.8.0'  

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    f'org.apache.kafka:kafka-clients:{kafka_version}'
]

spark = SparkSession.builder     .master("local")     .appName("kafka-example")     .config("spark.jars.packages", ",".join(packages))     .getOrCreate()

spark


# In[ ]:




