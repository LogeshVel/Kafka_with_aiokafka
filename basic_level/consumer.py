from aiokafka import AIOKafkaConsumer
import asyncio
from prettytable import PrettyTable
import subprocess, os, sys

t = PrettyTable()


async def consume_msg():
    t.field_names = ["Topic", "Partition", "Offset", "Key", "Value", "Timestamp ms", "Consumer Group"]
    if group_id == None:
        consumer = AIOKafkaConsumer(topic, bootstrap_servers='localhost:9092',auto_offset_reset="earliest")
        print(f"Started Consuming message for topic : {topic}")
    else:
        consumer = AIOKafkaConsumer(topic, group_id=group_id, bootstrap_servers='localhost:9092')
        print(f"Started Consuming message for topic : {topic}, Consumer Group : {group_id}")
    try:
        await consumer.start()
        async for msg in consumer:
            t.add_row([msg.topic, msg.partition, msg.offset, msg.key, msg.value,msg.timestamp, consumer._group_id])
            subprocess.call("clear" if os.name == "posix" else "cls")
            print(t)
    except KeyboardInterrupt as kint:
        print("Stop Consuming")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            topic = sys.argv[1]
            group_id = None
            asyncio.run(consume_msg())
        elif len(sys.argv) == 3:
            topic = sys.argv[1]
            group_id = sys.argv[2]
            asyncio.run(consume_msg())
        elif len(sys.argv) < 3:
            print("usage: consumer.py <topic> <group_id>")
            sys.exit(1)
    except KeyboardInterrupt as kint:
        print("Stop Consuming")

    print("Done Consuming")
