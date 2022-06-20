package com.lizza.base.producer.partitions;

import cn.hutool.core.util.RandomUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

/**
 * 自定义分区器
 * 场景: 主题 topic-1 中会同步所有订单数据, 根据业务诉求:
 * 1. 酒店类(hotel)订单数据同步到分区 1
 * 2. 机票类(flight)订单数据同步到分区 2
 * 3. 使用 key 来区分酒店/机票订单; 其他订单数据同步到 0 分区
 */
public class CustomPartitionerTests {

    @Test
    public void test1() throws Exception {
        // 创建 producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        String[] keys = new String[]{"hotel", "flight", "ticket", "other"};
        for (int i = 0; i < 11; i++) {
            String key = keys[RandomUtil.randomInt(4)];
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    // 主题
                    "topic-1",
                    key,
                    "data: " + key

            );
            // 同步和异步输出的分区号顺序不一致, 同步发送输出的顺序的分区号, 异步输出的倒序的分区号
            producer.send(record, (data, e) -> {
                System.out.println("key: " + key + ", partition: " + data.partition());
            }).get();
        }

        // 关闭资源
        producer.close();
    }

    @Test
    public void test2() throws Exception {
        // 创建 producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        for (int i = 0; i < 13; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    // 主题
                    "topic-1",
                    // 分区
                    i % 4,
                    // key
                    "key-" + i,
                    // 数据
                    "data-" + i

            );
            // 同步和异步输出的分区号顺序不一致, 同步发送输出的顺序的分区号, 异步输出的倒序的分区号
            producer.send(record, (data, e) -> {
                System.out.println("data: " + data + ", partition: " + data.partition());
            }).get();
        }

        // 关闭资源
        producer.close();
    }

}
