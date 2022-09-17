from aiokafka import AIOKafkaConsumer
import asyncio
from prettytable import PrettyTable
import subprocess, os

t = PrettyTable()


async def consume_msg():
    print("Started Consuming")
    try:
        t.field_names = ["Topic", "Partition", "Offset", "Key", "Value", "Timestamp ms"]
        consumer = AIOKafkaConsumer(
            "topic_one",
            bootstrap_servers='localhost:9092'
        )
        await consumer.start()
        async for msg in consumer:
            t.add_row([msg.topic, msg.partition, msg.offset, msg.key, msg.value,msg.timestamp])
#            print(
 #               "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
  #              msg.topic, msg.partition, msg.offset, msg.key, msg.value,
   #             msg.timestamp)
    #        )
            subprocess.call("clear" if os.name == "posix" else "cls")
            print(t)
    except KeyboardInterrupt as kint:
        print("Stop Consuming")
    finally:
        await consumer.stop()
try:
    asyncio.run(consume_msg())
except KeyboardInterrupt as kint:
    print("Stop Consuming")

print("Done Consuming")
