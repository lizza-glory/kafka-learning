package com.lizza.spring.kafka.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.lizza.spring.kafka.consumer.util.Constants.GROUP_ORDER_GROUP;
import static com.lizza.spring.kafka.consumer.util.Constants.TOPIC_QUICK_START;

@Slf4j
@Component
public class KafkaQuickStartConsumer {

    @KafkaListener(
            topics = TOPIC_QUICK_START,
            groupId = GROUP_ORDER_GROUP,
            concurrency = "5"/*,
            topicPartitions = {
                    @TopicPartition(topic = TOPIC_QUICK_START, partitions = {"0"}),
                    @TopicPartition(topic = TOPIC_QUICK_START, partitions = {"1"}),
            }*/)
    public void handle(ConsumerRecord<String, String> record) {
        log.info(record.value());
    }
}
