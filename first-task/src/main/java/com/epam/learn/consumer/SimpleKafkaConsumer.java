package com.epam.learn.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static com.epam.learn.KafkaUtil.setClosingShutdownHookForConsumer;

public class SimpleKafkaConsumer {

    private final String bootstrapServer;
    private static final String CONSUMER_GROUP = "first-task-consumer";
    private static final String AUTO_OFFSET_OPTION = "earliest";

    private KafkaConsumer<String, String> consumer;

    private Logger logger;

    public SimpleKafkaConsumer(String bootstrapServer, Logger logger) {
        this.bootstrapServer = bootstrapServer;
        initConsumer();
        this.logger = logger;
    }

    public void startConsoleConsumer(String topic, long msgLimit) {
        consumer.subscribe(Collections.singleton(topic));

        long msgCounter = 0;
        while (msgCounter < msgLimit) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> message : records) {
                String recordInfo = "Key: " + message.key() + " Value: " + message.value() +
                        " Partition: " + message.partition() + " Offset: " + message.offset();
                System.out.println(recordInfo);
                logger.info(recordInfo);

                msgCounter++;
            }
        }
    }

    private void initConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_OPTION);
        this.consumer = new KafkaConsumer<>(properties);
        setClosingShutdownHookForConsumer(this.consumer);
    }
}
