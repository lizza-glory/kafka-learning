package com.lizza.partitions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

/**
 * 默认分区器: DefaultPartitioner
 */
@Slf4j
public class DefaultPartitionTests {

    // 测试指定分区号
    @Test
    public void test1() throws Exception {
        // 创建 producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        for (int i = 0; i < 4; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    // 主题
                    "topic-1",
                    // 分区
                    i,
                    "",
                    "hello kafka~"

            );
            // 同步和异步输出的分区号顺序不一致, 同步发送输出的顺序的分区号, 异步输出的倒序的分区号
            producer.send(record, (data, e) -> {
                System.out.println("partition: " + data.partition());
            }).get();
        }

        // 关闭资源
        producer.close();
    }

    // 测试根据 key 的 hashCode 发送分区
    @Test
    public void test2() throws Exception {
        // 创建 producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        for (int i = 0; i < 7; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    // 主题
                    "topic-1",
                    // 分区
                    "key" + i,
                    "hello kafka~"

            );
            // 同步和异步输出的分区号顺序不一致, 同步发送输出的顺序的分区号, 异步输出的倒序的分区号
            producer.send(record, (data, e) -> {
                System.out.println("partition: " + data.partition());
            }).get();
        }

        // 关闭资源
        producer.close();
    }

    // 测试轮询方式发送分区
    @Test
    public void test3() throws Exception {
        // 创建 producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        for (int i = 0; i < 11; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    // 主题
                    "topic-1",
                    "hello kafka~"

            );
            // 同步和异步输出的分区号顺序不一致, 同步发送输出的顺序的分区号, 异步输出的倒序的分区号
            producer.send(record, (data, e) -> {
                System.out.println("partition: " + data.partition());
            }).get();
        }

        // 关闭资源
        producer.close();
    }
}
