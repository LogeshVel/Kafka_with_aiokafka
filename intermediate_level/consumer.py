from aiokafka import AIOKafkaConsumer, TopicPartition
import asyncio
from prettytable import PrettyTable
import subprocess, os, sys

t = PrettyTable()


async def consume_msg():
    t.field_names = ["Topic", "Partition", "Offset", "Key", "Value", "Timestamp ms", "Consumer Group"]
    if group_id is None or group_id.lower() == "none":
        consumer = AIOKafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset="earliest")
        print(f"Started Consuming message for topic(s) : {topics}")
    else:
        consumer = AIOKafkaConsumer(group_id=group_id, bootstrap_servers='localhost:9092')
        print(f"Started Consuming message for topic (s): {topics}, Consumer Group : {group_id}")

    try:
        await consumer.start()
        consumer.subscribe(topics)
        async for msg in consumer:
            t.add_row([msg.topic, msg.partition, msg.offset, msg.key, msg.value,msg.timestamp, consumer._group_id])
            subprocess.call("clear" if os.name == "posix" else "cls")
            print(t)
    except KeyboardInterrupt as kint:
        print("Stop Consuming")
    finally:
        await consumer.stop()

# Comsume the messages from the topics mentioned
if __name__ == "__main__":
    try:
        if len(sys.argv) < 3:
            print("usage: consumer.py <group_id> <topic_1> <topic_2> ...")
            print("mandatory args - group_id and one topic atleast. If you don't want to provide group id then given None")
            sys.exit(1)
        group_id = sys.argv[1]
        topics = sys.argv[2:]
        asyncio.run(consume_msg())
    except KeyboardInterrupt as kint:
        print("Stop Consuming")

    print("Done Consuming")
