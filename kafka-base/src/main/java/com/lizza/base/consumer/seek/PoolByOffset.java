package com.lizza.base.consumer.seek;

import cn.hutool.core.collection.CollectionUtil;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Properties;
import java.util.Set;

/**
 * 消费指定 offset 的消息
 */
public class PoolByOffset {

    @Test
    public void test1() throws Exception {
        // 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-2");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Lists.newArrayList("topic-1"));

        // 获取分区(consumer 分区消费方案具有时延, 所以需要轮询等待)
        Set<TopicPartition> partitions = consumer.assignment();
        while (CollectionUtil.isEmpty(partitions)) {
            partitions = consumer.assignment();
            consumer.poll(1000);
        }

        // 设置 offset
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, 20);
        }

        // 消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
