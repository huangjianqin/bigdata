package org.bigdata.kafka.multithread.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Created by 37 on 2017/6/22.
 */
public class StrUtil {
    /**
     * 生成topicX-partitionX,topicX-partitionX,......字符串
     * @param topicPartitions
     * @return
     */
    public static String topicPartitionsStr(Collection<TopicPartition> topicPartitions){
        if(topicPartitions != null && topicPartitions.size() > 0){
            StringBuilder sb = new StringBuilder();
            TopicPartition[] topicPartitionsArr = new TopicPartition[topicPartitions.size()];
            topicPartitions.toArray(topicPartitionsArr);
            sb.append(topicPartitionsArr[0].topic() + "-" + topicPartitionsArr[0].partition());
            for(int i = 1; i < topicPartitionsArr.length; i++){
                sb.append(", " + topicPartitionsArr[i].topic() + "-" + topicPartitionsArr[i].partition());
            }
            return sb.toString();
        }

        return null;
    }

    /**
     * 生成topicX-partitionX(Offset),topicX-partitionX(Offset),......字符串
     * @param offsets
     * @return
     */
    public static String topicPartitionOffsetsStr(Map<TopicPartition, OffsetAndMetadata> offsets){
       if(offsets != null && offsets.size() > 0){
           StringBuilder sb = new StringBuilder();
           Map.Entry<TopicPartition, OffsetAndMetadata>[] offsetEntryArr = new Map.Entry[offsets.size()];
           offsets.entrySet().toArray(offsetEntryArr);
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
        
        return null;
    }

    /**
     * 生成ConsumerRecord的具体信息字符串
     * @param record
     * @return
     */
    public static String consumerRecordDetail(ConsumerRecord record){
        String topic = record.topic();
        int partition = record.partition();
        Object key = record.key();
        Object value = record.value();
        return key + " >>> " + value + "(" + topic + "-" + partition + ")";
    }

}
