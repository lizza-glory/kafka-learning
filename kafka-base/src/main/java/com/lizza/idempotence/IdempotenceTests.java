package com.lizza.idempotence;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class IdempotenceTests {

    @Test
    public void test1() throws Exception {
        // producer 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 配置 ack 级别
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 配置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 配置重试次数
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 创建消息体
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "topic-1",
                0,
                System.currentTimeMillis(),
                "custom-key",
                "idempotence",
                Lists.newArrayList(
                        new RecordHeader("api-key", "123456".getBytes(StandardCharsets.UTF_8))
                )
        );

        // 创建 producer, 发送消息
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(record);

        // 关闭资源
        producer.close();
    }
}
