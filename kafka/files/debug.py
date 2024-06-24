from kafka import KafkaConsumer
import json
import report_pb2

broker = ['localhost:9092']
report = report_pb2.Report()

consumer = KafkaConsumer(
    # topic='temperatures',
    group_id='debug',
    bootstrap_servers=broker,
    auto_offset_reset='latest',
    # enable_auto_commit=False,
    # value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

consumer.subscribe('temperatures')

try:
    while(True):
        batch = consumer.poll(1000)
        for topic,message in batch.items():
            for mess in message:
                partition = topic.partition
                key = mess.key.decode('utf-8')
                date = report.FromString(mess.value).date
                degrees = report.FromString(mess.value).degrees
                
                print({'partition': partition, 'key': key, 'date': date, 'degrees': degrees})
    
except KeyboardInterrupt:
    consumer.close()