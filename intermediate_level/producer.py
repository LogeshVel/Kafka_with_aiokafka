from aiokafka import AIOKafkaProducer
import asyncio, random


# Produce message to the Topic with the value and key you provide.
async def produce_msg():
    print("Started Producing")
    
    try:
        while True:
            producer = AIOKafkaProducer()
            await producer.start()
            batch = producer.create_batch()
            t_k_v = input("enter-topic:key:value> ")
            tkv_list = t_k_v.split(":")
            try:
                topic = tkv_list[0]
                key = tkv_list[1]
                value = tkv_list[2]
                print(f"\ttopic {topic}, key {key}, value {value}")
                metadata = batch.append(key=bytes(key, 'utf-8'), value=bytes(value, 'utf-8'), timestamp=None)
                
                if metadata is None:
                    print("metadata is None")
            except IndexError as ie:
                print("\tPlease enter in the Format - Topic:Key:Value Ex: topic1:key1:value1")
                await producer.stop()
                continue
            partitions = await producer.partitions_for(topic)
            partition = random.choice(tuple(partitions))
            await producer.send_batch(batch, topic, partition=partition)
            print(f"\t{batch.record_count()} messages sent to partition {partition}")
            await producer.stop()
    except KeyboardInterrupt as kint:
        print("Stop producing")
    finally:
        await producer.stop()

asyncio.run(produce_msg())

print("Done producing")

