package com.lizza.performance;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.LocalTime;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * batch.size
 * linger.ms
 */
public class CustomConfigTests {

    @Test
    public void test0() throws Exception {
        System.out.println("hello".getBytes().length);
    }

    /**
     * 实践一: producer 发送消息, 如果消息的大小没有达到 batch.size, 但是
     * linger.ms 时间到了, 仍然会进行发送
     */
    @Test
    public void test1() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 20);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5 * 1000);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "topic-1",
                0,
                System.currentTimeMillis(),
                "key-1",
                "hello"
        );

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Future send = producer.send(record);
        printTime();
        System.out.println(send.get());
        producer.close();
    }

    private void printTime() {
        new Thread(() -> {
            while (true) {
                try {
                    System.out.println(LocalTime.now().withNano(0));
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "thread-1").start();
    }
}
