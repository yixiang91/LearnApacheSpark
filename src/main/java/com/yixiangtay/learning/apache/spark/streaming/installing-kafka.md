# Installing Kafka (on Windows)

### Pre-requisite(s)

* 7-zip

> https://www.apache.org/dyn/closer.cgi?path=/kafka/2.1.0/kafka_2.11-2.1.0.tgz

### Commands

```bash
# STEP #1 (start zookeeper)
zookeeper-server-start.bat ..\..\config\zookeeper.properties

# STEP #2 (start kafka)
kafka-server-start.bat ..\..\config\server.properties

# STEP #3 (create topic)
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic viewrecords

# STEP #4 (list topics)
kafka-topics.bat --list --zookeeper localhost:2181

# STEP #5 (start a consumer)
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic viewrecords
```

### Troubleshoot

> Error: missing `server' JVM at `C:\Program Files (x86)\Java\jre1.8.0_151\bin\server\jvm.dll'.
  Please install or use the JRE or JDK that contains these missing components.

Resolution
* Copy contents of C:\Program Files (x86)\Java\jre1.8.0_151\bin\client to C:\Program Files (x86)\Java\jre1.8.0_151\bin\server

> Error: java.io.IOException: Map failed

Resolution
* Use 64-bit JDK instead of 32-bit JDK