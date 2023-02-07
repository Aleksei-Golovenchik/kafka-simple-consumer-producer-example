package com.epam.learn.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

import static com.epam.learn.KafkaUtil.handleCliInput;
import static com.epam.learn.KafkaUtil.redirectLogOutputToFile;

public class ProducerRunner {
    public static void main(String[] args) {
        redirectLogOutputToFile();
        final SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer("localhost:29092");
        handleCliInput( value -> {
            RecordMetadata metadata = simpleKafkaProducer.sendMessage("test", value);
            System.out.println("Message sent: " + value +
                    " offset: " + metadata.offset() +
                    " partition: " + metadata.partition());
        });
    }
}
