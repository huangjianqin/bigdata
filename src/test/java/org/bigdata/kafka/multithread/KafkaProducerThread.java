package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bigdata.kafka.api.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hjq on 2017/6/22.
 */
public class KafkaProducerThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerThread.class);
    private KafkaProducer<String, String> producer;
    private long producerId;
    private String topic;
    private boolean isStopped = false;
    static Long offset = -1L;

    public KafkaProducerThread(KafkaProducer<String, String> producer, String topic, long producerId) {
        this.producer = producer;
        this.topic = topic;
        this.producerId = producerId;
    }

    public void close(){
        this.isStopped = true;
    }

    @Override
    public void run() {
//        System.out.println(topic);
//        for(int i = 0; i < 200000; i++){
//            final String msg = "producer-" + producerId + " message" + Counters.getCounters().get("producer-counter");
//            producer.send(new ProducerRecord<String, String>(topic, (int) (Counters.getCounters().get("producer-counter") % 10), null, msg), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    Counters.getCounters().add("producer-counter");
//                    Counters.getCounters().add("producer-byte-counter", msg.getBytes().length);
//
//                    synchronized (offset) {
//                        if (recordMetadata.offset() > offset) {
//                            offset = recordMetadata.offset();
//                        }
//                    }
//                }
//            });
//        }
//
//----------------------------------------------------------------------------------------------------------------------------------
//
        while (!isStopped && !Thread.currentThread().isInterrupted()) {
            final String msg = "producer-" + producerId + " message" + Counters.getCounters().get("producer-counter");
            //log.cleaner.enable=true
            //log.cleanup.policy=compact
            //使用compact的时候，需要根据record 的key（每条recored 的key都不一样）来进行压缩，如果发送消息时key为null，则发送不了消
            producer.send(new ProducerRecord<String, String>(topic, (int) (Counters.getCounters().get("producer-counter") % 10), System.currentTimeMillis(), System.currentTimeMillis() + offset + "", msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    Counters.getCounters().add("producer-counter");
                    Counters.getCounters().add("producer-byte-counter", msg.getBytes().length);

                    synchronized (offset) {
                        if (recordMetadata.offset() > offset) {
                            offset = recordMetadata.offset();
                        }
                    }
                }
            });
//            try {
//                Thread.sleep(50);
//            } catch (InterruptedException e) {
//                break;
//            }
        }
    }
}
