package com.lizza.base.producer.sync;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * kafka 发送同步消息
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        // 创建 producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 发送消息
        ProducerRecord<String, String> record = new ProducerRecord<>(
                // 主题
                "topic-1",
                // 分区
                0,
                // 发送消息的时间戳, 默认也是 System.currentTimeMillis()
                System.currentTimeMillis(),
                "custom-key",
                "hello kafka~",
                // header
                Lists.newArrayList(
                        new RecordHeader("api-key", "123456".getBytes(StandardCharsets.UTF_8))
                )

        );
        Future<RecordMetadata> future = producer.send(record, (data, e) -> {
            System.out.println("data: " + data + ", e: " + e);
        });
        // 调用 get 方法表示同步发送, 如果没有调用, 则表示异步
        System.out.println("future: " + future.get());

        // 关闭资源
        producer.close();
    }
}
