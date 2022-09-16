from kafka import KafkaConsumer
from pymongo import MongoClient


CONSUMER = KafkaConsumer(
    bootstrap_servers='10.10.15.72:9092,10.10.15.73:9092,10.10.15.74:9092',
    group_id='tour_recommend_anhdang',
    auto_offset_reset='latest',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='admin-hahalolo',
    sasl_plain_password='Hahalolo@2021'
    )

CONSUMER.subscribe(["topic_social_post_ai.test-api-social.post"])

MONGODB_URI = "mongodb+srv://anhdang000:anhdang000@cluster0.maozpf4.mongodb.net/?retryWrites=true&w=majority"
CLIENT = MongoClient(MONGODB_URI)
DB = CLIENT['test']