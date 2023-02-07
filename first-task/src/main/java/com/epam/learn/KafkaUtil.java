package com.epam.learn;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.impl.SimpleLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class KafkaUtil {

    private static final String PATH_TO_LOG_TEMP_FILE = "tmp/log.log";
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_YELLOW = "\u001B[33m";

    private KafkaUtil(){}

    public static void redirectLogOutputToFile() {
        System.setProperty(SimpleLogger.LOG_FILE_KEY, PATH_TO_LOG_TEMP_FILE);
    }

    public static void setClosingShutdownHook(AutoCloseable closeable) {

        Runtime.getRuntime().addShutdownHook(new Thread(getRunnableOnClosing(closeable)));
    }

    public static <K,V> void setClosingShutdownHookForConsumer(KafkaConsumer<K,V> consumer) {
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(getRunnableOnConsumerClosing(consumer, main)));
    }

    private static Runnable getRunnableOnClosing(AutoCloseable closeable) {
        return () -> {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            printLogs();
        };
    }

    private static <K,V> Runnable getRunnableOnConsumerClosing(KafkaConsumer<K,V> consumer, Thread thread) {
        return () -> {
            try {
                consumer.wakeup();
                thread.join(100);
                consumer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            printLogs();
        };
    }

    private static void printLogs() {
        if (PATH_TO_LOG_TEMP_FILE.equals(System.getProperty(SimpleLogger.LOG_FILE_KEY))) {
            System.out.println(ANSI_YELLOW + "\nRecorded log output:\n" + ANSI_RESET);
            try (Stream<String> stream = Files.lines(Paths.get(PATH_TO_LOG_TEMP_FILE))) {
                System.out.println(stream.collect(Collectors.joining("\n")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void handleCliInput(Consumer<String> consumer) {
        Scanner scanner = new Scanner(System.in);
        String inputValue = "";
        System.out.println("insert value (type \"exit\" to finish)");
        inputValue = scanner.nextLine();
        while (!inputValue.equalsIgnoreCase("exit")) {
            consumer.accept(inputValue);
            inputValue = scanner.nextLine();
        }
    }

}
