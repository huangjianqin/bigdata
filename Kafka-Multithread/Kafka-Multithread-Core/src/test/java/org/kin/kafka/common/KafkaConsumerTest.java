package org.kin.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.utils.PropertiesWrapper;

import java.util.*;

/**
 * Created by 健勤 on 2017/8/5.
 */
public class KafkaConsumerTest {
    public static void main(String[] args) throws InterruptedException {
        /**
         * 1.验证是否可以动态订阅topic
         * 2.pause方法校验
         */
        Properties config = PropertiesWrapper.create()
                .set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092")
                .set(ConsumerConfig.GROUP_ID_CONFIG, "multi-handle")
                .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .properties();
        Set<String> topic = new HashSet<>();
        topic.add("msg1");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer(config);
        consumer.subscribe(topic);
        consumer.poll(0);
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition("msg1", 0)));
        printTopicPartition(consumer.poll(1000));
        topic = new HashSet<String>();
        topic.add("msg2");
        consumer.subscribe(topic);
        consumer.poll(0);
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition("msg2", 0)));
        printTopicPartition(consumer.poll(1000));

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                List<TopicPartition> l = new ArrayList<>();
//                l.add(new TopicPartition("msg1", 0));
//                l.add(new TopicPartition("msg1", 1));
                l.add(new TopicPartition("msg2", 0));
                while(true && !Thread.currentThread().isInterrupted()){
                     consumer.seekToBeginning(l);
                     printTopicPartition(consumer.poll(500));
                      try {
                          Thread.sleep(1000);
                      } catch (InterruptedException e) {
                          e.printStackTrace();
                      }
              }
            }
        });
        thread.start();

        Thread.sleep(3000);
        consumer.pause(Collections.singletonList(new TopicPartition("msg2", 0)));
        System.out.println("暂停..");
        Thread.sleep(10000);
        consumer.resume(Collections.singletonList(new TopicPartition("msg2", 0)));
        System.out.println("恢复..");
        Thread.sleep(10000);
        thread.interrupt();

        consumer.close();
    }

    public static void printTopicPartition(ConsumerRecords<String, String> records){
        System.out.println(records.count());
        for(TopicPartition topicPartition: records.partitions()){
            System.out.print(topicPartition.toString() + ", ");
        }
        System.out.println();
    }
}
