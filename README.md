# Kafka-stream-uppercase
````
this service will recevice the message from upstream , process the message to uppercase
and transform to downstream topic

````

# Kafka - configuration

# start zookeeper
````
zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties

````
# start kafka broker
````
kafka-server-start.bat %KAFKA_HOME%\config\server.properties

kafka-server-start.bat %KAFKA_HOME%\config\server1.properties

````

# topic 
````
create up stream topic upstream-message

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic upstream-message --config min.insync.replicas=2


create down stream topic downstream-message

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic downstream-message

kafka-topics.bat  --alter --zookeeper localhost:2181 --topic downstream-message --config min.insync.replicas=2


````

# produce console message in upstream-message topic
````
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic upstream-message

kafka-console-producer.bat --broker-list localhost:9092 --topic upstream-message
````

# consumer console message in downstream-message topic

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic downstream-message  --from-beginning