package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by hjq on 2017/6/21.
 */
public class DefaultCommitStrategy implements CommitStrategy{
    private long counter = 0L;
    private long MAX_COUNT = 50;

    public DefaultCommitStrategy() {
    }

    public DefaultCommitStrategy(long MAX_COUNT) {
        this.MAX_COUNT = MAX_COUNT;
    }

    @Override
    public void setup() throws Exception {

    }

    @Override
    public boolean isToCommit(ConsumerRecord record) {
        if(++counter % MAX_COUNT == 0){
            counter = 0L;
            return true;
        }

        return false;
    }

    @Override
    public synchronized void reset() {
        counter = 0L;
    }

    @Override
    public void cleanup() throws Exception {

    }
}
