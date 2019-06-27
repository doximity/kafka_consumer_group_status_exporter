# kafka_status_exporter
Kafka status exporter for Prometheus.

### Compiling

This uses Maven, so you would simply compile this by doing this at the root of the project: `mvn package`

### Running

Currently only Java 1.8 is supported. To run this, you would do the following:

```
java -jar kafka_status_exporter-0.1.0-jar-with-dependencies.jar --bootstrap-servers=<kafka-hostname>:9092 --port <port>
```

You can also run this to print out the help usage:

```
java -jar kafka_status_exporter-0.1.0-jar-with-dependencies.jar --help
```

### TODO

* Add unit testing
* Separate some of the code under the run() function into their own class (Gauge etc)
* Establish / make sure semver is used.
* Wiki documentation
* Anything else I can't think of that someone will find. :)

