package com.epam.learn.consumer;

import org.slf4j.LoggerFactory;

import static com.epam.learn.KafkaUtil.redirectLogOutputToFile;

public class ConsumerRunner {


    public static void main(String[] args) {
        redirectLogOutputToFile();
        new SimpleKafkaConsumer("localhost:29092", LoggerFactory.getLogger(SimpleKafkaConsumer.class))
                .startConsoleConsumer("test", Long.MAX_VALUE);
    }
}