from kafka import KafkaConsumer
import json
import time


class ConsumerServer():

    def __init__(self, topic, broker, group_id):
        self.topic = topic
        self.broker = broker
        self.group_id = group_id

    def consume_data(self):
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=self.broker,
                                 group_id=self.group_id,
                                 enable_auto_commit=True)
        
        record_number = 1
        for message in consumer:
            record_dict = json.loads(message.value.decode('utf-8'))
            print(f"\nReceived Record : {record_number} Data : {record_dict}")
            record_number = record_number + 1
            time.sleep(1)
            
if __name__ == '__main__':
    obj = ConsumerServer('com.udacity.sf.crime.rate', ['localhost:9092'], 'consumer-group-1')
    obj.consume_data()
        