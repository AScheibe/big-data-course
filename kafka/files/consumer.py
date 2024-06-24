import json
import os
import argparse
from kafka import KafkaConsumer, TopicPartition
import datetime
import report_pb2

# Kafka setup
broker = 'localhost:9092'
topic = 'temperatures'

# Parse command line arguments
parser = argparse.ArgumentParser(description="Consume temperature data from Kafka.")
parser.add_argument('partitions', nargs='+', type=int, help='List of partitions to consume from.')
args = parser.parse_args()
report = report_pb2.Report()

def load_partition_data(partitions):
    partition_data = {}
    for partition in partitions:
        filename = os.path.join('/files', f'partition-{partition}.json')
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                partition_data[partition] = json.load(f)
        else:
            # Initialize new partition data structure
            partition_data[partition] = {"partition": partition, "offset": 0}
    return partition_data

def save_partition_data(partition, data):
    path = os.path.join('/files', f'partition-{partition}.json')
    path_tmp = path + ".tmp"
    with open(path_tmp, "w") as f:
        json.dump(data, f)
    os.rename(path_tmp, path)

def update_statistics(data, message):
    # Parse report from message value
    report.ParseFromString(message.value)
    month = message.key.decode('utf-8')
    date = report.FromString(message.value).date
    year = datetime.datetime.strptime(date, '%Y-%m-%d').year
    degrees = int(report.FromString(message.value).degrees)

    if month not in data:
        data[month] = {}

    if year not in data[month]:
        data[month][year] = {
            'count': 1,
            'sum': degrees,
            'avg': degrees,
            'start': date,
            'end': date
        }
    else:
        year_data = data[month][year]
        year_data['count'] += 1
        year_data['sum'] += degrees
        year_data['avg'] = year_data['sum'] / year_data['count']
        year_data['end'] = report.date

def main():
    print("running")
    consumer = KafkaConsumer(
        bootstrap_servers=[broker]
    )
    consumer.assign([TopicPartition(topic, p) for p in args.partitions])

    partition_data = load_partition_data(args.partitions)
    seen_messages = set()

    for partition, data in partition_data.items():
        tp = TopicPartition(topic, partition)
        consumer.seek(tp, data['offset'])

    try:
        while True:
            batch = consumer.poll(1000)
            for topic_partition, messages in batch.items():
                for message in messages:
                    message_id = report.FromString(message.value).date
                    if message_id in seen_messages:
                        continue  # Skip processing if it's a duplicate
                    seen_messages.add(message_id)

                    print(topic_partition.partition)
                    partition = topic_partition.partition

                    update_statistics(partition_data[partition], message)

                    # inc offset
                    partition_data[partition]['offset'] = message.offset + 1
                    save_partition_data(partition, partition_data[partition])
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()
