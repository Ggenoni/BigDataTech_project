from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('processed-data', bootstrap_servers='kafka:9092', auto_offset_reset='latest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    data = message.value
    if data['fire_risk'] > 0.8:
        alert = {"location": data['location'], "risk": data['fire_risk']}
        producer.send('fire-alerts', alert)
