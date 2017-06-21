package org.bigdata.kafka.multithread;

import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by hjq on 2017/6/19.
 * 对TopicPartition的简单封装,添加appendTime属性以作为加入队列的时间并以该属性值排序
 * equals和hashcode都是直接使用TopicPartition的实现
 */
public class TopicPartitionWithTime implements Comparable<TopicPartitionWithTime>{
    private TopicPartition topicPartition;
    private long appendTime = 0;

    public TopicPartitionWithTime(TopicPartition topicPartition, long appendTime) {
        this.topicPartition = topicPartition;
        this.appendTime = appendTime;
    }

    public TopicPartition getTopicPartition() {
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

    @Override
    public int compareTo(TopicPartitionWithTime o) {
        if(o == null){
            return 1;
        }
        if(o.getAppendTime() < getAppendTime()){
            return 1;
        }
        else if(o.getAppendTime() == getAppendTime()){
            return 0;
        }
        else{
            return -1;
        }
    }
}
