package com.lizza.base.consumer;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Properties;

public class BaseApiTests {

    /**
     * 基础 api 使用
     */
    @Test
    public void test1() throws Exception {

        // 1. 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");


        // 2. 创建 consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅主题
        consumer.subscribe(Lists.newArrayList("quickstart"));

        // 4. 拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    /**
     * 单个消费者消费指定分区的数据
     */
    @Test
    public void test2() throws Exception {

        // 1. 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");


        // 2. 创建 consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅主题
        consumer.assign(Lists.newArrayList(
                new TopicPartition("topic-1", 3)
        ));

        // 4. 拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    /**
     * 消费者组
     */
    @Test
    public void test2() throws Exception {

        // 1. 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");


        // 2. 创建 consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅主题
        consumer.assign(Lists.newArrayList(
                new TopicPartition("topic-1", 3)
        ));

        // 4. 拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

}
