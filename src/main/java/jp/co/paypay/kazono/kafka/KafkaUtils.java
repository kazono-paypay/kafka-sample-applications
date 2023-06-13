package jp.co.paypay.kazono.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {
    private Logger logger = LogManager.getLogger(KafkaUtils.class);
    private AdminClient adminClient;
    private static final Long DELETE_TIMEOUT_SECOND = 10l;

    public KafkaUtils(String brokers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        adminClient = AdminClient.create(props);
    }

    public void createTopic(String topic, int partition, int replicationFactor) {
        logger.info("Creating topic { name: {}, partitions: {}, replicationFactor: {}}",
                topic, partition, replicationFactor);

        if (listTopic().contains(topic)) {
            logger.warn("Topic '{}' is already exists.", topic);
            return;
        }

        NewTopic newTopic = new NewTopic(topic, partition, (short)replicationFactor);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
            logger.info("Created Kafka Topic: {}", topic);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> listTopic() {
        Set<String> topics = new HashSet<>();
        try {
            topics = adminClient.listTopics().names().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return topics;
    }
}
