import datetime
from gcn_kafka import Consumer
from confluent_kafka import TopicPartition

consumer = Consumer(client_id='1k3ffap716hdphq8pu0vjvu2dv',
                    client_secret='1q5eo7ah7cs53dg0va2pjsb9h2i16oig74phurcuj3bprhe9voga',
                    domain='gcn.nasa.gov')

# get messages occurring 3 days ago
timestamp1 = int((datetime.datetime.now() - datetime.timedelta(days=3)).timestamp() * 1000)
timestamp2 = timestamp1 + 3*86400000 # +1 day

topic = 'gcn.classic.voevent.INTEGRAL_SPIACS'
start = consumer.offsets_for_times(
    [TopicPartition(topic, 0, timestamp1)])
end = consumer.offsets_for_times(
    [TopicPartition(topic, 0, timestamp2)])

consumer.assign(start)
try:
    consumer.assign(start)
    for message in consumer.consume(end[0].offset - start[0].offset, timeout=1):
        print(message.value())
## Except to ignore error that occurs when there are no new alerts
except ValueError: 
    pass