package com.doximity.kafka.kafka_consumer_group_status_exporter;

import com.doximity.kafka.kafka_consumer_group_status_exporter.Instrumentation.ConsumerGroupExtractor;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests Prometheus HTTP Server as well as backend Kafka calls to get consumer groups and their status.
 */
public class ConsumerGroupExporterTest {
    String testTopic = "test_topic";
    HTTPServer httpServer;
    KafkaConsumer<String, String> consumer;
    Integer port = 1234;

    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupExporterTest.class);

    // Creates and stands up an internal test kafka cluster to be shared across test cases within this test class.
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource().withBrokers(1);

    /**
     * Simple accessor to the getKafkatestUtils object to interact with the test cluster.
     * @return Returns a SharedKafkaTestResource.getKafkaTestUtils accessor object.
     */
    protected KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

    protected String getBootStrapServers() {
        return sharedKafkaTestResource.getKafkaConnectString();
    }

    public static boolean contains(final int[] arr, final int key) {
        return Arrays.stream(arr).anyMatch(i -> i == key);
    }

    void startHTTPServer() {
        try {
            logger.info("Starting Prometheus HTTP Server on port " + port);
            httpServer = new HTTPServer(port);
        } catch (Exception e) {
            logger.error("Unable to register Prometheus HTTP Server on port " + port, e);
        }
    }

    void startConsumer(String bootStrapServers) {
        consumer = TestConsumer.createConsumer(bootStrapServers);
        consumer.subscribe(Arrays.asList(testTopic));

        // We need to call poll() multiple times to give time for the group to be registered with the group coordinator.
        for (int i = 0; i < 5; i++) {
            consumer.poll(Duration.ofMillis(100));
        }
    }

    /**
     * Simple function to parse the output of the Prometheus HTTP Server
     * @param suffix Any suffix we want to provide to focus on a specific label
     * @return Returns the response from the Prometheus HTTP Server
     * @throws IOException
     */
    public String request(String suffix) throws IOException {
        String url = "http://localhost:1234/metrics" + suffix;
        URLConnection connection = new URL(url).openConnection();
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.connect();
        // The "\\A" delimiter indicates start of a string.
        Scanner s = new Scanner(connection.getInputStream(), "UTF-8").useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    /**
     * Test the functionality of the Prometheus HTTP Server as well as the ConsumerGroupExtractor methods that populate it.
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     */
    @Test
    void getConsumerGroupResultsTest() throws InterruptedException, ExecutionException, IOException {
        int[] statusCodes = {0,1,2,3,4,5,99};
        String bootStrapServers = getBootStrapServers();
        Gauge cgStatus = Gauge.build()
                .name("kafka_consumer_group_status")
                .labelNames("group_id")
                .help("Reports Consumer Group status.").register();

        getKafkaTestUtils().createTopic(testTopic, 1, (short) 1);
        startConsumer(bootStrapServers);
        startHTTPServer();

        Map<String, Integer> consumerGroupsResultsFinal = null;
        consumerGroupsResultsFinal = ConsumerGroupExtractor.getConsumerGroupResults(bootStrapServers);

        for (Map.Entry<String, Integer> consumerGroupStatus : consumerGroupsResultsFinal.entrySet()) {
            cgStatus.labels(consumerGroupStatus.getKey()).set(consumerGroupStatus.getValue());

            String response = request("");

            assertTrue(response.contains("kafka_consumer_group_status{group_id=\"exporter-cg-group-id\",}"));
            assertTrue(consumerGroupStatus.getKey().equals("exporter-cg-group-id"));
            assertTrue(contains(statusCodes, consumerGroupStatus.getValue()));
        }

        consumer.close();
        httpServer.stop();
    }
}
