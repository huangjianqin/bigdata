package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by hjq on 2017/6/21.
 */
public class DefaultCommitStrategy implements CommitStrategy{
    private AtomicLong counter = new AtomicLong(0);
    private long MAX_COUNT = 10;

    public DefaultCommitStrategy() {
    }

    public DefaultCommitStrategy(long MAX_COUNT) {
        this.MAX_COUNT = MAX_COUNT;
    }

    @Override
    public boolean isToCommit(ConsumerRecord record) {
        if(counter.incrementAndGet() >= MAX_COUNT){
            return true;
        }

        return false;
    }
}
