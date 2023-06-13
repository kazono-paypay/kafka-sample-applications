package jp.co.paypay.kazono;

import jp.co.paypay.kazono.kafka.Consumer;
import jp.co.paypay.kazono.kafka.KafkaProperties;
import jp.co.paypay.kazono.kafka.KafkaUtils;
import jp.co.paypay.kazono.kafka.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException, TimeoutException {

        logger.info("***** Initializing sample-kafka-application *****");
        KafkaProperties kafkaProps = new KafkaProperties();
        String bootstrapServers = kafkaProps.getKAFKA_BROKER_HOST() + ":" + kafkaProps.getKAFKA_BROKER_PORT();
        CountDownLatch countDownLatch = new CountDownLatch(2);
        /*
        KafkaUtils utils = new KafkaUtils(bootstrapServers);
        utils.createTopic(kafkaProps.getTOPIC(), kafkaProps.getPARTITION(), kafkaProps.getREPLICATION_FACTOR());

        Producer producer = new Producer(bootstrapServers,
                kafkaProps.getTOPIC(),
                600_000L, //Produce messages for 1 minute.
                countDownLatch
                );
        producer.start();

         */
        Consumer consumer = new Consumer(bootstrapServers,
                kafkaProps.getTOPIC(),
                kafkaProps.getCONSUMER_GROUP_ID(),
                100,
                countDownLatch);
        consumer.start();

        if (!countDownLatch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }
        consumer.shutdown();
    }
}
