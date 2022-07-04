package com.lizza.base.consumer.seek;

import cn.hutool.core.collection.CollectionUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * 消费指定时间点之后的数据
 */
public class PoolByTime {

    @Test
    public void test1() throws Exception {
        // 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-3");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Lists.newArrayList("topic-1"));

        // 获取分区(consumer 分区消费方案具有时延, 所以需要轮询等待)
        Set<TopicPartition> partitions = consumer.assignment();
        while (CollectionUtil.isEmpty(partitions)) {
            partitions = consumer.assignment();
            consumer.poll(1000);
        }

        // 根据时间换 offset
        Map<TopicPartition, Long> timestampsToSearch = Maps.newHashMap();

        // 设置 offset
        for (TopicPartition partition : partitions) {
            timestampsToSearch.put(partition, System.currentTimeMillis() - 40 * 60 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(timestampsToSearch);

        for (TopicPartition partition : partitions) {
            OffsetAndTimestamp offsetAndTimestamp = map.get(partition);
            if (Objects.nonNull(offsetAndTimestamp)) {
                consumer.seek(partition, offsetAndTimestamp.offset());
            }
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
