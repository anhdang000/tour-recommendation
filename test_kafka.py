import json

from kafka import KafkaConsumer
import pymongo


consumer = KafkaConsumer(
    bootstrap_servers='10.10.15.72:9092,10.10.15.73:9092,10.10.15.74:9092',
    group_id='tour_recommend_anhdang',
    auto_offset_reset='latest',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='admin-hahalolo',
    sasl_plain_password='Hahalolo@2021'
    )

consumer.subscribe(["topic_social_post_ai.test-api-social.post"])
print(f'Subscribed to topic')
while True:
    for msg in consumer:
        msg = json.loads(msg.value.decode("utf-8"))
        print(msg)
        with open('post_log.json', 'w') as f:
            json.dump(msg, f)