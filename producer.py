from aiokafka import AIOKafkaProducer
import asyncio

async def produce_msg():
    print("Started Producing")
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()
    try:
        while True:
            msg = input("enter_msg> ")
            await producer.send_and_wait("topic_one", bytes(msg, 'utf-8'))
    except KeyboardInterrupt as kint:
        print("Stop producing")
    finally:
        await producer.stop()

asyncio.run(produce_msg())

print("Done producing")
