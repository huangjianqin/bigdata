package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.consumer.multi.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * 进一步抽象kafka提供的ConsumerRebalanceListener
 * <p>
 * Created by 健勤 on 2017/7/26.
 */
public abstract class AbstractConsumerRebalanceListener implements ConsumerRebalanceListener {
    protected static Logger log = LoggerFactory.getLogger(AbstractConsumerRebalanceListener.class);
    /** 绑定的runner */
    protected OCOTMultiProcessor.KafkaMessageHandlerRunner runner;

    public AbstractConsumerRebalanceListener() {
    }

    public AbstractConsumerRebalanceListener(OCOTMultiProcessor.KafkaMessageHandlerRunner runner) {
        this.runner = runner;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
        log.info("message processor-{} onPartitionsRevoked", runner.getRunnerId());
        log.info("message processor-{} origin assignments --->> {}", runner.getRunnerId(), TPStrUtils.topicPartitionsStr(topicPartitions));
        //提交最新处理的Offset
        runner.commitOffset();

        doOnPartitionsRevoked(topicPartitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
        log.info("message processor-{} onPartitionsAssigned", runner.getRunnerId());
        log.info("message processor-{} new assignments --->> {}", runner.getRunnerId(), TPStrUtils.topicPartitionsStr(topicPartitions));

        doOnPartitionsAssigned(topicPartitions);
    }

    /**
     * 初始化
     */
    public abstract void setup();

    /**
     * kafka consumer rebalance处理
     */
    public abstract void doOnPartitionsRevoked(Collection<TopicPartition> topicPartitions);

    /**
     * kafka consumer reassigned处理
     */
    public abstract void doOnPartitionsAssigned(Collection<TopicPartition> topicPartitions);

    /**
     * 释放资源
     */
    public abstract void cleanup();
}
