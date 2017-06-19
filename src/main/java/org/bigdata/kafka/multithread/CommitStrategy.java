package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by 37 on 2017/6/19.
 */
public interface CommitStrategy {
    boolean isToCommit(ConsumerRecord record);
}
