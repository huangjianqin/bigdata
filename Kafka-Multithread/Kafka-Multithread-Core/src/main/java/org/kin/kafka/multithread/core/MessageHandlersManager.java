package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.CommitStrategy;
import org.kin.kafka.multithread.configcenter.ReConfigable;
import org.kin.kafka.multithread.utils.ConsumerRecordInfo;
import org.kin.kafka.multithread.api.MessageHandler;

import java.util.Map;
import java.util.Set;

/**
 * Created by hjq on 2017/7/4.
 */
public interface MessageHandlersManager extends ReConfigable {
    void registerHandlers(Map<String, Class<? extends MessageHandler>> topic2HandlerClass);
    void registerCommitStrategies(Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass);
    boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets);
    void consumerCloseNotify();

    /**
     * 提交当前分配到的所有分区的最新Offset
     * @param topicPartitions 当前分配到的分区
     */
    void consumerRebalanceNotify(Set<TopicPartition> topicPartitions);

    /**
     * 主要是清理负责之前分配到但此次又没有分配到的TopicPartition对应的message handler及一些资源
     * 新分配到的TopicPartition(之前没有的)由dispatch方法新分配资源
     * @param topicPartitions 之前分配到但此次又没有分配到的TopicPartitions
     */
    void doOnConsumerReAssigned(Set<TopicPartition> topicPartitions);

    /**
     * 更新配置,如果callback发生变化,需要更新所有未处理消息的callback实例
     */
}
