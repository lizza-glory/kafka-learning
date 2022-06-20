package com.lizza.base.producer.transaction;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

/**
 * 事务
 */
public class TransactionTests {

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
        // 开启幂等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        // 事务 id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        // 创建消息体
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "topic-1",
                0,
                System.currentTimeMillis(),
                "custom-key",
                "transaction",
                Lists.newArrayList(
                        new RecordHeader("api-key", "123456".getBytes(StandardCharsets.UTF_8))
                )
        );

        // 创建 producer, 发送消息
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 初始化事务
        producer.initTransactions();
        // 开启事务
        producer.beginTransaction();
        try {
            producer.send(record);
            // 业务代码异常
            exec();
            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            // 放弃事务
            producer.abortTransaction();
        } finally {
            // 关闭资源
            producer.close();
        }
    }

    private void exec() {
        throw new RuntimeException("模拟程序异常");
    }
}
