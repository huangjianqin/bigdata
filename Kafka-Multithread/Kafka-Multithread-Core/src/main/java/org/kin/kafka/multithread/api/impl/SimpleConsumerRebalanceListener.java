package org.kin.kafka.multithread.api.impl;

import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.AbstractConsumerRebalanceListener;
import org.kin.kafka.multithread.core.OCOTMultiProcessor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 健勤 on 2017/7/27.
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
        for(TopicPartition topicPartition: topicPartitions){
            nowOffsets.put(topicPartition, processor.position(topicPartition));
        }
    }

    @Override
    public void doOnPartitionsAssigned(Collection<TopicPartition> topicPartitions) throws Exception {
        for(TopicPartition topicPartition: topicPartitions){
            if(nowOffsets.containsKey(topicPartition)){
                processor.seekTo(topicPartition, nowOffsets.get(topicPartition));
            }
        }
    }

    @Override
    public void cleanup() throws Exception {

    }
}
