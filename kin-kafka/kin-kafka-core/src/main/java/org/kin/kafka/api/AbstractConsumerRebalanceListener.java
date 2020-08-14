package org.kin.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.core.OCOTMultiProcessor;
import org.kin.kafka.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by 健勤 on 2017/7/26.
 */
public abstract class AbstractConsumerRebalanceListener implements ConsumerRebalanceListener {
    protected static Logger log = LoggerFactory.getLogger(AbstractConsumerRebalanceListener.class);
    protected OCOTMultiProcessor.OCOTProcessor processor;

    public AbstractConsumerRebalanceListener() {
    }

    public AbstractConsumerRebalanceListener(OCOTMultiProcessor.OCOTProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
        //提交最新处理的Offset
        processor.commitLatest();
        log.info("message processor-" + processor.getProcessorId() + " onPartitionsRevoked");
        log.info("message processor-" + processor.getProcessorId() + " origin assignments --->> " + TPStrUtils.topicPartitionsStr(topicPartitions));
        try {
            doOnPartitionsRevoked(topicPartitions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
        log.info("message processor-" + processor.getProcessorId() + " onPartitionsAssigned");
        log.info("message processor-" + processor.getProcessorId() + " new assignments --->> " + TPStrUtils.topicPartitionsStr(topicPartitions));
        try {
            doOnPartitionsAssigned(topicPartitions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract void setup() throws Exception;

    public abstract void doOnPartitionsRevoked(Collection<TopicPartition> topicPartitions) throws Exception;

    public abstract void doOnPartitionsAssigned(Collection<TopicPartition> topicPartitions) throws Exception;

    public abstract void cleanup() throws Exception;
}
