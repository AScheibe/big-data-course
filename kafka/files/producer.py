from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather
import time
import report_pb2
import calendar

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(5) # Deletion sometimes takes a while to reflect

topic_name = "temperatures"
new_topic = NewTopic(name=topic_name, num_partitions=4, replication_factor=1)

admin_client.create_topics([new_topic])

print("Topics:", admin_client.list_topics())

producer = KafkaProducer(
    bootstrap_servers=broker,
    retries=10,
    acks='all'
)

for date, degrees in weather.get_next_weather(delay_sec=0.1):
    report = report_pb2.Report()
    report.date = date
    report.degrees = degrees

    message_value = report.SerializeToString()

    message_key = calendar.month_name[int(date.split('-')[1])]
    print(message_key)

    producer.send(topic_name, key=message_key.encode('utf-8'), value=message_value)

producer.close()

