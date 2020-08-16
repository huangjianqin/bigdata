package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Offset提交策略,只要规则通过才提交相应的Offset
 * <p>
 * Created by hjq on 2017/6/19.
 */
public interface OffsetCommitStrategy<K, V> {
    /**
     * 初始化
     */
    void setup(KafkaFetchConfig<K, V> fetchConfig);

    /**
     * 判断是否满足自定义规则,满足则返回true
     */
    boolean isToCommit(KafkaMessageHandler<K, V> kafkaMessageHandler, ConsumerRecord<K, V> record);

    /**
     * stop并释放资源
     */
    void shutdown();

}
