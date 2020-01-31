package com.doximity.kafka.kafka_status_exporter;

/*
 * Entry point for this project.
 *
 * @author Dennis Bielinski
 */

import com.doximity.kafka.kafka_status_exporter.Instrumentation.ConsumerGroupExtractor;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@CommandLine.Command(name = "ConsumerGroupExporter")
public class ConsumerGroupExporter implements Runnable {
    @Option(names = {"-b", "--bootstrap-servers"}, required = true, description = "Kafka brokers to connect to.")
    String bootstrapServers;

    @Option(names = {"-p", "--port"}, required = true, description = "Metrics server port for Prometheus to scrape.")
    Integer port;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Print usage help and exit.")
    boolean usageHelpRequested;

    // Creating and registering the Gauge metric we'll be using
    protected static final Gauge cgStatus = Gauge.build()
            .name("kafka_consumer_group_status")
            .labelNames("group_id")
            .help("Reports Consumer Group status.").register();

    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupExporter.class);

    // The function that main will call after a successful parsing of the command line.
    public void run() {
        try {
            logger.info("Starting Prometheus HTTP Server on port " + port);
            HTTPServer server = new HTTPServer(port);
        } catch (Exception e) {
            logger.error("Unable to register Prometheus HTTP Server on port " + port, e);
        }

        Map<String, Integer> consumerGroupsResultsFinal = null;

        while (true) {
            try {
                consumerGroupsResultsFinal = ConsumerGroupExtractor.getConsumerGroupResults(bootstrapServers);

                if (consumerGroupsResultsFinal != null) {
                    for (Map.Entry<String, Integer> consumerGroupStatus : consumerGroupsResultsFinal.entrySet()) {
                        cgStatus.labels(consumerGroupStatus.getKey()).set(consumerGroupStatus.getValue());
                    }
                }

                Thread.sleep(5000);
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Main function, parses the command line options and will perform the run() function if args are correct.
     * Automatically determines if -h / --help flags are called.
     *
     * @param args The args passed through the command line that picocli will parse.
     */
    public static void main(String[] args) {
        new CommandLine(new ConsumerGroupExporter()).parseWithHandler(new CommandLine.RunLast(), System.err, args);
    }
}
