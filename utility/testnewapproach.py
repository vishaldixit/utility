from kafka import TopicPartition
from kafka.consumer import KafkaConsumer
topic = 'targetedwireviewtopiclongrunv1'
brokers = '172.50.41.61:6667,172.50.41.62:6667,172.50.41.63:6667'
con = KafkaConsumer(bootstrap_servers=brokers)
ps = [TopicPartition(topic, p) for p in con.partitions_for_topic(topic)]

print(con.end_offsets(ps))
print(con.beginning_offsets(ps))



# from kafka import KafkaConsumer
# consumer = KafkaConsumer('targetedwireviewtopiclongrunv1', bootstrap_servers='172.50.41.61:6667,172.50.41.62:6667,172.50.41.63:6667')
# print(consumer.beginning_offsets(['0', '1']))
# for msg in consumer:
#     print(msg)
