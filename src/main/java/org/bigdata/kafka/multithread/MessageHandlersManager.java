package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Created by hjq on 2017/7/4.
 */
public interface MessageHandlersManager {
    void registerHandlers(Map<String, MessageHandler> topic2Handler);
    void registerCommitStrategies(Map<TopicPartition, CommitStrategy> topic2CommitStrategy);
    boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets);
    void consumerCloseNotify(Set<TopicPartition> topicPartitions);
    void consumerRebalanceNotify();
}
