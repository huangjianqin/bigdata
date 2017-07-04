package org.bigdata.kafka.multithread;


import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hjq on 2017/7/4.
 */
public abstract class AbstractMessageHandlersManager implements MessageHandlersManager {
    private static Logger log = LoggerFactory.getLogger(AbstractMessageHandlersManager.class);
    protected Map<TopicPartition, CommitStrategy> topic2CommitStrategy = new ConcurrentHashMap<>();
    protected Map<String, MessageHandler> topic2Handler = new ConcurrentHashMap<>();
    protected AtomicBoolean isRebalance = new AtomicBoolean(false);

    public void registerHandler(String topic, MessageHandler handler){
        try {
            if (topic2Handler.containsKey(topic)){
                topic2Handler.get(topic).cleanup();
            }
            handler.setup();
            topic2Handler.put(topic, handler);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void registerHandlers(Map<String, MessageHandler> topic2Handler){
        if(topic2Handler == null){
            return;
        }
        for(Map.Entry<String, MessageHandler> entry: topic2Handler.entrySet()){
            registerHandler(entry.getKey(), entry.getValue());
        }
    }

    public void registerCommitStrategy(TopicPartition topicPartition, CommitStrategy strategy){
        try {
            if (topic2CommitStrategy.containsKey(topicPartition)){
                topic2CommitStrategy.get(topicPartition).cleanup();
            }
            strategy.setup();
            topic2CommitStrategy.put(topicPartition, strategy);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void registerCommitStrategies(Map<TopicPartition, CommitStrategy> topic2CommitStrategy){
        if(topic2CommitStrategy == null){
            return;
        }
        for(Map.Entry<TopicPartition, CommitStrategy> entry: topic2CommitStrategy.entrySet()){
            registerCommitStrategy(entry.getKey(), entry.getValue());
        }
    }

    protected void cleanMsgHandlersAndCommitStrategies(){
        log.info("cleaning message handlers & commit stratgies...");
        try {
            //清理handler和commitstrategy
            for(MessageHandler messageHandler: topic2Handler.values()){
                messageHandler.cleanup();
            }

            for(CommitStrategy commitStrategy: topic2CommitStrategy.values()){
                commitStrategy.cleanup();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("message handlers & commit stratgies cleaned");
    }

}
