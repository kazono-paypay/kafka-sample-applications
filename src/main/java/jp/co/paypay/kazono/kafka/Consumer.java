package jp.co.paypay.kazono.kafka;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer extends ShutdownableThread {
    private static final Logger logger = LogManager.getLogger(Consumer.class);
    private KafkaConsumer consumer;
    private String topic;
    private String consumerGroupId;
    private int numMessageToConsume;
    private int messageRemaining;
    private CountDownLatch countDownLatch;

    public Consumer(String broker,
                    String topic,
                    String consumerGroupId,
                    int numMessageToConsume,
                    CountDownLatch countDownLatch) {
        super("KafkaConsumerSample", false);

        logger.info("***** Initializing Kafka Consumer *****");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.numMessageToConsume = numMessageToConsume;
        this.messageRemaining = numMessageToConsume;
        this.countDownLatch = countDownLatch;

        logger.info("***** Completed initializing phase for Kafka Consumer *****");
    }

    @Override
    public void doWork() {
        logger.info("Consume records from Kafka Topic: {}.", topic);

        /*
        long start = System.currentTimeMillis();
        consumer.subscribe(Collections.singleton(this.topic));
        ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Long, String> record : records) {
            System.out.println("{ \"Key\": " + record.key()
                    + ", \"Message\": " + record.value()
                    + ", \"Partition\": " + record.partition()
                    + "}");
        }
        consumer.commitAsync();
        numMessageToConsume -= records.count();
        if (numMessageToConsume <= 0) {
            consumer.close();
            countDownLatch.countDown();
            logger.info("Processing Time[Kafka Client]: " + (System.currentTimeMillis() - start) + "[ms]");
        }
         */

        consumer.subscribe(Collections.singleton(this.topic));
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Long, String> record : records) {
                System.out.println("{ \"Key\": " + record.key()
                        + ", \"Message\": " + record.value()
                        + ", \"Partition\": " + record.partition()
                        + "}");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
