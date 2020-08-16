package org.kin.kafka.consumer.multi.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.kin.framework.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author huangjianqin
 * @date 2020/8/16
 */
public class KafkaUtils {
    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * 设置topic partition开始消费的offset
     */
    public static <K, V> void setupKafkaStartOffset(KafkaConsumer<K, V> consumer, String topicPartitionOffsetStr) {
        if (StringUtils.isNotBlank(topicPartitionOffsetStr)) {
            StringBuffer sb = new StringBuffer();
            for (String topicPartitionOffset : topicPartitionOffsetStr.split(",")) {
                String[] splits = topicPartitionOffset.split(":");
                String topic = splits[0];
                int partition = Integer.parseInt(splits[1]);
                long startOffset = Long.parseLong(splits[2]);
                if (startOffset > 0) {
                    consumer.seek(new TopicPartition(topic, partition), startOffset);
                    sb.append(topic.concat("-").concat(Integer.toString(partition)).concat(":").concat(Long.toString(startOffset)).concat(","));
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            log.info("kafka consumer begin fetcher message from >>> {}", sb.toString());
        }
    }
}
