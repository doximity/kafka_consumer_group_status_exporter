Changelog
=========
## 0.3.0 2/3/2020
   * Added unit testing and also embedded Salesforce JUnit Kafka framework
   * Fixed typo in getConsumerGroupResults() function
   * Fixed "timed out waiting for node assignment" bug when a broker is restarted by making sure
     that the exception is bubbled up to main() and program is exited.

## 0.2.0 7/1/2019
   * Moved from Jetty web server to Prometheus' built-in HTTP Server to simplify things.

## 0.1.0 6/26/2019
   * Adding baseline functionality.
