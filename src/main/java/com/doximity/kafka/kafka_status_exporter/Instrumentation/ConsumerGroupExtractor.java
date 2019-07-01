package com.doximity.kafka.kafka_status_exporter.Instrumentation;

/*
 * Consumer Group extraction / instrumentation.
 *
 * @author Dennis Bielinski
 */

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerGroupExtractor {

    /**
     * Returns an AdminClient object that is connected to the Kafka environment.
     *
     * @param bootstrapServers The Kafka brokers to connect to. E.g. "kafka1:9092,kafka2:9092,kafka3:9092"
     *
     * @return The AdminClient object to use to establish a session into the Kafka environment.
     */
    private static AdminClient createAdminClient(String bootstrapServers) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }

    /**
     * Represents information about a Consumer Group, returning its ID.
     */
    public static class ConsumerGroupIdentifier {
        private final String id;

        /**
         * Constructor.
         * @param id consumer group id/name.
         */
        public ConsumerGroupIdentifier(final String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Override
        public String toString() {
            return id;
        }
    }

    /**
     * Gets the current state of the consumer group in question.
     *
     * @param adminClient The AdminClient object to use to connect to Kafka to get the information.
     * @param cgId The Consumer Group ID in question.
     * @return Returns the current state of the Consumer Group.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static ConsumerGroupState extractConsumerGroupStatus(AdminClient adminClient, String cgId) throws ExecutionException, InterruptedException {

        DescribeConsumerGroupsResult describeConsumerGroupResults = null;
        try {
            describeConsumerGroupResults = adminClient.describeConsumerGroups(Collections.singletonList(cgId));
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert describeConsumerGroupResults != null;
        KafkaFuture<Map<String, ConsumerGroupDescription>> result = describeConsumerGroupResults.all();
        Map<String, ConsumerGroupDescription> cgDescriptions = result.get();
        ConsumerGroupDescription cgDescription = cgDescriptions.get(cgId);

        return cgDescription.state();
    }

    /**
     * Lists all Consumer Groups that are currently in the environment and extracts their ID and State into a Hashtable.
     *
     * @param bootstrapServers The Kafka brokers to connect to. E.g. "kafka1:9092,kafka2:9092,kafka3:9092"
     * @return Returns a Hashtable of each Consumer Group ID and their State as a value.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Map<String, Integer> getConsumerGroupResults(String bootstrapServers) throws ExecutionException, InterruptedException {

        Map<String, Integer> consumerGroupsResultsFinal = null;
        try {
            AdminClient adminClient = createAdminClient(bootstrapServers);

            ListConsumerGroupsResult groups = adminClient.listConsumerGroups();
            List<ConsumerGroupIdentifier> consumerIds = new ArrayList<>();

            groups.all().get().forEach((result) -> {
                consumerIds.add(
                        new ConsumerGroupIdentifier(result.groupId())
                );
            });

            consumerIds.sort(Comparator.comparing(ConsumerGroupIdentifier::getId));

            consumerGroupsResultsFinal = new HashMap<String, Integer>();

            for (ConsumerGroupIdentifier group : consumerIds) {
                String grp = String.valueOf(group);
                if (grp.contains("console-consumer")) {
                    continue;
                }

                String status = String.valueOf(extractConsumerGroupStatus(adminClient, grp));

                /*
                Mapping strings to numerical values to ingest into Prometheus which will be re-mapped back in Grafana.
                The reason to use Prometheus is to track the status over a range of time per group.
                Prometheus does not accept strings as values, so they need to be mapped to an int.

                Here are the mappings of all possible CG states in the ConsumerGroupState object:

                5 = Unknown, 4 = Stable, 3 = CompletingRebalance, 2 = PreparingRebalance, 1 = Empty, 0 = Dead.
                */

                switch (status) {
                    case "Unknown":
                        consumerGroupsResultsFinal.put(grp, 5);
                        break;
                    case "Stable":
                        consumerGroupsResultsFinal.put(grp, 4);
                        break;
                    case "CompletingRebalance":
                        consumerGroupsResultsFinal.put(grp, 3);
                        break;
                    case "PreparingBalance":
                        consumerGroupsResultsFinal.put(grp, 2);
                        break;
                    case "Empty":
                        consumerGroupsResultsFinal.put(grp, 1);
                        break;
                    case "Dead":
                        consumerGroupsResultsFinal.put(grp, 0);
                        break;
                }
            }

            adminClient.close();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return consumerGroupsResultsFinal;
    }
}
