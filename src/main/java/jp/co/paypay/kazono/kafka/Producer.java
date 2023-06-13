package jp.co.paypay.kazono.kafka;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
    private final Logger logger = LogManager.getLogger(Producer.class);
    private KafkaProducer<Long, String> producer;
    private String topic;
    private Long interval;
    private CountDownLatch countDownLatch;

    public Producer(String broker,
                    String topic,
                    Long interval,
                    CountDownLatch countDownLatch) {
        logger.info("***** Initializing Kafka Producer *****");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

        this.topic = topic;
        this.interval = interval;
        this.countDownLatch = countDownLatch;

        logger.info("***** Completed initializing phase for Kafka Producer *****");
    }

    @SneakyThrows
    @Override
    public void run() {
        int messageSuffix = 0;
        int numRecordsProduced = 0;

        logger.info("Send records to Kafka Topic: {}.", topic);
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < interval) {
            String message = "Message_" + messageSuffix;
            Long timestamp = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(
                    topic,
                    timestamp,
                    message
            )).get();
            System.out.println("{ Key: " + timestamp + ", Value: " + message + " }");
            Thread.sleep(1000);

            messageSuffix++;
            numRecordsProduced++;
        }
        logger.info("Producer sent {} record successfully", numRecordsProduced);
        countDownLatch.countDown();
    }
}
