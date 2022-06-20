package com.lizza.base.producer.partitions;

import cn.hutool.core.util.StrUtil;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 * 场景: 主题 topic-1 中会同步所有订单数据, 根据业务诉求, 酒店类订单数据同步到分区 1,
 * 机票类订单数据同步到分区 2, 使用 key 来区分酒店/机票订单; 其他订单数据同步到 0 分区
 */
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (!StrUtil.equals(topic, "topic-1")) {
            return 0;
        }
        if (StrUtil.equals(key.toString(), "hotel")) {
            return 1;
        }
        if (StrUtil.equals(key.toString(), "flight")) {
            return 2;
        }
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
