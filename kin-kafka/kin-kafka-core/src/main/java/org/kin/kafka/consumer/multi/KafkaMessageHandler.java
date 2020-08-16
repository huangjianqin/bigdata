package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * kafka消息处理具体逻辑
 * <p>
 * Created by hjq on 2017/6/19.
 */
public interface KafkaMessageHandler<K, V> {
    /**
     * 初始化
     */
    void setup(KafkaFetchConfig<K, V> fetchConfig);

    /**
     * 消息处理
     */
    void handle(ConsumerRecord<K, V> record);

    /**
     * stop, 释放占用资源
     */
    void shutdown();
}
