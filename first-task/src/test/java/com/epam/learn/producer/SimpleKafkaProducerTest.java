package com.epam.learn.producer;

import com.epam.learn.consumer.SimpleKafkaConsumer;
import com.github.javafaker.Faker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleKafkaProducerTest {

    private static final String KAFKA_CONTAINER_IMAGE = "confluentinc/cp-kafka:7.0.1";
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(KAFKA_CONTAINER_IMAGE));
    private static final String TEST_TOPIC = "testTopic";
    private final Faker faker = new Faker();

    @BeforeAll
    static void startKafka() {
        kafka.start();
    }

    @Test
    void sendingAndReceiveMessagesTest() {
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(kafka.getBootstrapServers());
        List<String> testMessages = Stream.generate(() -> faker.hobbit().quote()).limit(10).collect(Collectors.toList());
        testMessages.forEach(message -> simpleKafkaProducer.sendMessage(TEST_TOPIC, message));

        Logger mock = Mockito.mock(Logger.class);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(kafka.getBootstrapServers(), mock);
        simpleKafkaConsumer.startConsoleConsumer(TEST_TOPIC, 10);
        Mockito.verify(mock, Mockito.times(10)).info(captor.capture());
        List<String> receivedMessages = captor.getAllValues().stream()
                .map(recordInfo -> recordInfo.split("Value: ")[1].split(" Partition:")[0])
                .collect(Collectors.toList());

        Assertions.assertEquals(testMessages, receivedMessages);
    }


}