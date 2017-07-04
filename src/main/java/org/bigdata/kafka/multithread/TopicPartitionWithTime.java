package org.bigdata.kafka.multithread;

import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by hjq on 2017/6/19.
 */
public class TopicPartitionWithTime{
    private TopicPartition topicPartition;
    private long appendTime = 0;

    public TopicPartitionWithTime(TopicPartition topicPartition, long appendTime) {
        this.topicPartition = topicPartition;
        this.appendTime = appendTime;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public long getAppendTime() {
        return appendTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartitionWithTime)) return false;

        TopicPartitionWithTime that = (TopicPartitionWithTime) o;

        return !(topicPartition != null ? !topicPartition.equals(that.topicPartition) : that.topicPartition != null);

    }

    @Override
    public int hashCode() {
        return topicPartition != null ? topicPartition.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TopicPartitionWithTime{" +
                "topicPartition=" + topicPartition +
                ", appendTime=" + appendTime +
                '}';
    }
}
