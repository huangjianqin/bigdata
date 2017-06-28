package org.bigdata.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bigdata.kafka.multithread.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

/**
 * Created by hjq on 2017/6/22.
 */
public class KafkaProducerThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerThread.class);
    private KafkaProducer<String, String> producer;
    private long producerId;
    private String topic;
    private boolean isStopped = false;

    public KafkaProducerThread(Properties properties, String topic, long producerId) {
        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
        this.producerId = producerId;
    }

    public void close(){
        this.isStopped = true;
    }

    @Override
    public void run() {
        try{
            while(!isStopped && !Thread.currentThread().isInterrupted()){
                final String msg = "producer-" + producerId + " message" + Counters.getCounters().get("producer-counter");
                producer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                        System.out.println("producer-" + producerId + " send message[ " + msg + " ]");
                    }
                });
                Counters.getCounters().add("producer-counter");
//                try {
//                    Thread.sleep(200);
//                } catch (InterruptedException e) {
//                    break;
//                }
            }
        }
        finally {
            producer.close();
            log.info("producer closed");
        }
    }
}
