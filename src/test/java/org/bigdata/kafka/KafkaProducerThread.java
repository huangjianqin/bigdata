package org.bigdata.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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

    public KafkaProducerThread(Properties properties, String topic, long producerId) {
        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
        this.producerId = producerId;
    }

    @Override
    public void run() {
        try{
            int count = 0;
            while(true){
                final String msg = "prodcuer-" + producerId + " message" + count;
                producer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        System.out.println("producer-" + producerId + " send message[ " + msg + " ]");
                    }
                });
                count++;
            }
        }
        finally {
            producer.close();
            log.info("producer closed");
        }
    }
}
