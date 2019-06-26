package com.doximity.kafka.kafka_status_exporter;

/*
 * Entry point for this project.
 *
 * @author Dennis Bielinski
 */

import com.doximity.kafka.kafka_status_exporter.Instrumentation.ConsumerGroupExtractor;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
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

    // The function that main will call after a successful parsing of the command line.
    public void run() {

        Gauge cgStatus = Gauge.build()
                .name("kafka_consumer_group_status")
                .labelNames("group_id")
                .help("Reports Consumer Group status.").register();


        // The Jetty Servlet that serves the Consumer Group exporter and runs the bulk of the work.
        class CGServlet extends HttpServlet {

            @Override
            protected void doGet(final HttpServletRequest req,
                                 final HttpServletResponse resp)
                    throws ServletException, IOException {

                Map<String, Integer> consumerGroupsResultsFinal = null;

                try {
                    consumerGroupsResultsFinal = ConsumerGroupExtractor.getConsumerGroupResults(bootstrapServers);
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }

                if (consumerGroupsResultsFinal != null) {
                    for (Map.Entry<String, Integer> consumerGroupStatus : consumerGroupsResultsFinal.entrySet()) {
                        cgStatus.labels(consumerGroupStatus.getKey()).set(consumerGroupStatus.getValue());
                    }
                }

                resp.getWriter().println("Consumer Group Exporter. Metrics are located under /metrics");
            }
        }

        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new CGServlet()), "/");
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
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
