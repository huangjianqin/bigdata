package org.kin.kafka.api.impl;

import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.api.AbstractConsumerRebalanceListener;
import org.kin.kafka.core.OCOTMultiProcessor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 健勤 on 2017/7/27.
 * consumer rebalance时原子保存所有处理过的offset
 */
public class SimpleConsumerRebalanceListener extends AbstractConsumerRebalanceListener {
    private Map<TopicPartition, Long> nowOffsets;

    public SimpleConsumerRebalanceListener() {
    }

    public SimpleConsumerRebalanceListener(OCOTMultiProcessor.OCOTProcessor processor) {
        super(processor);
    }

    @Override
    public void setup() throws Exception {
        nowOffsets = new HashMap<>();
    }

    @Override
    public void doOnPartitionsRevoked(Collection<TopicPartition> topicPartitions) throws Exception {
        //保存已处理过的Offset
        for (TopicPartition topicPartition : topicPartitions) {
            nowOffsets.put(topicPartition, processor.position(topicPartition));
        }
    }

    @Override
    public void doOnPartitionsAssigned(Collection<TopicPartition> topicPartitions) throws Exception {
        //回复处理过的Offset
        for (TopicPartition topicPartition : topicPartitions) {
            if (nowOffsets.containsKey(topicPartition)) {
                processor.seekTo(topicPartition, nowOffsets.get(topicPartition));
            }
        }
        nowOffsets.clear();
    }

    @Override
    public void cleanup() throws Exception {

    }
}
