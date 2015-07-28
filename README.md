# Brief Overview

Spark-Camus is a Kafka -> HDFS pipeline on Spark platform. It includes the following features:
- using low level Kafka consumer API
- recording offsets of all partitions
- convert the Kafka message to com.tresata.spark.kafka.KafkaRDD
- call saveAsSequenceFile and save the messages to HDFS

# Configure file

```
# kafka topic information
topic=
partNum=
brokerPort=
metadata.broker.list=
auto.offset.reset=largest
fetch.message.max.bytes=31457280

# HDFS path
outputBaseDir=
lastPath=
currOffPath=

# offset record path
offsetDir=

# Kafka message format
reader=bytebuffer
```

# Running Spark-Camus

- first, running the com.td.kafka.offset.OffsetMonitor and recording the real-time offset
- create a new config.properties
- call CamusJob or overwrite the CamusJob