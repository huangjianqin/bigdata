package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Created by hjq on 2017/7/4.
 */
public interface MessageHandlersManager {
    void registerHandlers(Map<String, Class<? extends MessageHandler>> topic2HandlerClass);
    void registerCommitStrategies(Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass);
    boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets);
    void consumerCloseNotify(Set<TopicPartition> topicPartitions);
    void consumerRebalanceNotify();
}
