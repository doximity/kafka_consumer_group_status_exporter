# kafka_consumer_group_status_exporter
Kafka Consumer Group status exporter for Prometheus.

### Compiling

This uses Maven, so you would simply compile this by doing this at the root of the project: `mvn clean package`

### Running

Currently only Java 1.8 is supported. To run this, you would do the following:

```
java -jar kafka_consumer_group_status_exporter-0.3.0-jar-with-dependencies.jar --bootstrap-servers=<kafka-hostname>:9092 --port <port>
```

You can also run this to print out the help usage:

```
java -jar kafka_consumer_group_status_exporter-0.3.0-jar-with-dependencies.jar --help
```

### TODO

* Build out CI/CD pipeline for future PR's

