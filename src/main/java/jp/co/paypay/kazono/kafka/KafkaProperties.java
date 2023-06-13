package jp.co.paypay.kazono.kafka;

import lombok.Data;

@Data
public class KafkaProperties {
    public String TOPIC = "performance-test";
    public String KAFKA_BROKER_HOST = "localhost";
    public int KAFKA_BROKER_PORT = 19092;
    public int REPLICATION_FACTOR = 1;
    public int PARTITION = 2;
    public String CONSUMER_GROUP_ID = "performance-test-group";
}
