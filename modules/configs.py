from kafka import KafkaConsumer
from pymongo import MongoClient

MONGODB_URI = "mongodb+srv://anhdang000:anhdang000@cluster0.maozpf4.mongodb.net/?retryWrites=true&w=majority"

KAFKA_CFGS = dict(
	bootstrap_servers=['10.10.11.237:9094', '10.10.11.238:9094', '10.10.11.239:9094', '10.10.11.244:8080'], 
    group_id='log_collector_ai_da_hung', 
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='admin-hahalolo',
    sasl_plain_password='Hahalolo@2021'
)