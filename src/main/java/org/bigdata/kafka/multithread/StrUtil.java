package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Created by 37 on 2017/6/22.
 */
public class StrUtil {
    public static String topicPartitionsStr(Collection<TopicPartition> topicPartitions){
        StringBuilder sb = new StringBuilder();
        TopicPartition[] topicPartitionsArr = (TopicPartition[]) topicPartitions.toArray();
        sb.append(topicPartitionsArr[0].topic() + "-" + topicPartitionsArr[0].partition());
        for(int i = 1; i < topicPartitionsArr.length; i++){
            sb.append(", " + topicPartitionsArr[i].topic() + "-" + topicPartitionsArr[i].partition());
        }
        return sb.toString();
    }

    public static String topicPartitionOffsetsStr(Map<TopicPartition, OffsetAndMetadata> offsets){
        StringBuilder sb = new StringBuilder();
        Map.Entry<TopicPartition, OffsetAndMetadata>[] offsetEntryArr = (Map.Entry<TopicPartition, OffsetAndMetadata>[]) offsets.entrySet().toArray();
        String topic = offsetEntryArr[0].getKey().topic();
        int partition = offsetEntryArr[0].getKey().partition();
        long offset = offsetEntryArr[0].getValue().offset();
        sb.append(topic + "-" + partition + "(" + offset + ")");
        for(int i = 1; i < offsetEntryArr.length; i++){
            topic = offsetEntryArr[i].getKey().topic();
            partition = offsetEntryArr[i].getKey().partition();
            offset = offsetEntryArr[i].getValue().offset();
            sb.append(", " + topic + "-" + partition + "(" + offset + ")");
        }

        return sb.toString();
    }

    public static String consumerRecordDetail(ConsumerRecord record){
        String topic = record.topic();
        int partition = record.partition();
        Object key = record.key();
        Object value = record.value();
        return key + " >>> " + value + "(" + topic + "-" + partition + ")";
    }

}
