package com.lizza.base.consumer.concurrence;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Properties;

/**
 * 1. 一个 consumer, 多个线程去消费
 * 2. 多个 consumer, 多个线程去消费
 */
public class ConcurrentConsumers {

    @Test
    public void test1() throws Exception {
        // 1. 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ORDER_GROUP");

        // 2. 创建 consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅主题
        consumer.subscribe(Lists.newArrayList("QUICK_START"));

        // 4. 拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    @Test
    public void test3() throws Exception {
        // 1. 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ORDER_GROUP");

        // 2. 创建多个 consumer
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

                // 3. 订阅主题
                consumer.subscribe(Lists.newArrayList("QUICK_START"));

                // 4. 拉取数据
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record);
                    }
                }
            }, "thread-" + i).start();
        }
        System.in.read();
    }
}
