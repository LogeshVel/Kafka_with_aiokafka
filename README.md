# Kafka_with_aiokafka

Working with Kafka Producers and Consumers in Python using aiokafka library

## Installation

```pip install aiokafka```

## Start the Zookeeper and the kafka server (Broker)

```
zookeeper-server-start.sh ~/Desktop/kafka_2.13-3.2.1/config/zookeeper.properties

kafka-server-start.sh ~/Desktop/kafka_2.13-3.2.1/config/server.properties
```

## Python kafka producer and kafka consumer

**Once the zookeeper and kafka server was started then checking for the topics**

![image](https://user-images.githubusercontent.com/69865283/190843370-340ce79e-76ff-459a-998a-a715ca2c61d3.png)

At first topic is not created but once I have run the Consumer code then the topic is created. The Consumer or the Producer will look for the topic in kafka if not present then they will create one.

![image](https://user-images.githubusercontent.com/69865283/190843669-7510d552-599f-43b4-96af-b6c92def1b1d.png)

Produce and Consume

![image](https://user-images.githubusercontent.com/69865283/190843879-261e5cc2-9823-41e9-9215-f3dc31d28829.png)


## Consumer groups

![image](https://user-images.githubusercontent.com/69865283/190849258-56dfd4cc-d210-4238-b963-5332a4e4b1be.png)


![image](https://user-images.githubusercontent.com/69865283/190849304-3b13e598-e084-4352-b3d3-6d130749151d.png)


![image](https://user-images.githubusercontent.com/69865283/190849416-499718f3-967b-45f7-a335-f562f18844dc.png)

![image](https://user-images.githubusercontent.com/69865283/190849502-33f16503-057d-4d68-9e1a-740462052cd7.png)

## Produce and Consume key:value from/to Multiple topics 


![image](https://user-images.githubusercontent.com/69865283/190872150-3e17d71e-5781-409a-bf86-143b6fab3bd0.png)

