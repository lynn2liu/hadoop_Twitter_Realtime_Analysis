# RealtimeTwitterSentimentAnalyzer 

A realtime Sentiment Analyzer over a Twitter Stream using big data technologies of the Hadoop ecosystem with Spark Streaming, Flume, Kafka, and Flask. 

![Demo Page](./pictures/demo.png)

## Pipeline:

![image-20201110132218997](/Users/haileyliu/Library/Application Support/typora-user-images/image-20201110132218997.png)

## Data Ingesting with Flume:  

Ingest and filter tweets from Twitter API  
with customized event-driven source and interceptor by Flume. 
Then, all accepted tweets will end up on a Kafka topic, by configuring Kafka as a sink. 


1. Start the ZooKeeper and Kafka server.   
``` 
cd /usr/local/kafka 
bin/zookeeper-server-start.sh config/zookeeper.properties & 
bin/kafka-server-start.sh config/server.properties & 
```

2. Create a Kafka topic: 
``` 
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic twitter_stream & 
```

3. Start the Flume agent: 
```shell script 
flume-ng agent --conf /usr/local/flume/conf --conf-file flume_twitter_to_kafka.conf --name agent1 --plugins-path /usr/local/flume/plugins.d/  -Dflume.root.logger=INFO,console 
```

4. Start a Kafka consumer: 
```shell script 
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_stream --from-beginning 
```


## Sentiment Analysis with Spark Streaming:  

Use Spark Streaming with python to process and set the Spark Streaming Context with 2 seconds as a batch interval,  the latest tweets received during a window of the last 15 minutes.  

Calculate the most used hashtags, the users most mentioned, and the most active users.  
Perform a sentiment analysis by using the TextBlob library. 

Run Spark Streaming Project to listen Kakfa topic of twitter_stream: 

```shell script 
~ run localhost:9092 twitter_stream
```

## Dashboard Display with Flask:  

Flask Web Application for displaying the results using Chart.js. 

```shell script 
cd dashboard/TwitterAnalysisDashboard 
flask run -p 5001 
```

