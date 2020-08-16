package org.kin.kafka.consumer.multi;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * consumer rebalance时原子保存所有处理过的offset
 * <p>
 * Created by 健勤 on 2017/7/27.
 */
public class JvmConsumerRebalanceListener extends AbstractConsumerRebalanceListener {
    /** kafka consumer topic partition 消费到的offset */
    private volatile Map<TopicPartition, Long> nowOffsets;

    public JvmConsumerRebalanceListener() {
    }

    public JvmConsumerRebalanceListener(OCOTMultiProcessor.KafkaMessageHandlerRunner runner) {
        super(runner);
    }

    @Override
    public void setup() {
    }

    @Override
    public void doOnPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
        Map<TopicPartition, Long> nowOffsets = new HashMap<>();
        //保存已处理过的Offset
        for (TopicPartition topicPartition : topicPartitions) {
            nowOffsets.put(topicPartition, runner.position(topicPartition));
        }
        this.nowOffsets = nowOffsets;
    }

    @Override
    public void doOnPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
        Map<TopicPartition, Long> nowOffsets = new HashMap<>(this.nowOffsets);
        //恢复处理过的Offset
        for (TopicPartition topicPartition : topicPartitions) {
            if (nowOffsets.containsKey(topicPartition)) {
                runner.seekTo(topicPartition, nowOffsets.get(topicPartition));
            }
        }
    }

    @Override
    public void cleanup() {

    }
}
