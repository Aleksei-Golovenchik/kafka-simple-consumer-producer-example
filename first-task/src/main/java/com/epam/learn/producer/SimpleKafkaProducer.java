package com.epam.learn.producer;

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

import static com.epam.learn.KafkaUtil.setClosingShutdownHook;

@Getter
public class SimpleKafkaProducer {

    private final String bootstrapServer;
    private KafkaProducer<String, String> producer;

    public SimpleKafkaProducer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
        initProducer();
    }

    @SneakyThrows
    public RecordMetadata sendMessage(String topic, String message) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, String.valueOf(message.hashCode()), message));
        return future.get();
    }

    private void initProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(properties);
        setClosingShutdownHook(this.producer);
    }
}