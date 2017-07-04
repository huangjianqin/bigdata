package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Created by hjq on 2017/7/4.
 * OPMT ==> one partition more thread
 */
public class OPMTMessageHandlersManager extends AbstractMessageHandlersManager {

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets) {
        return false;
    }

    @Override
    public void consumerCloseNotify(Set<TopicPartition> topicPartitions) {

    }

    @Override
    public void consumerRebalanceNotify() {

    }
}
