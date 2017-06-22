package org.bigdata.kafka.api;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.bigdata.kafka.multithread.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by hjq on 2017/6/19.
 */
public class MultiThreadConsumerManager {
    private static Logger log = LoggerFactory.getLogger(MultiThreadConsumerManager.class);
    private Map<String, MessageFetcher> name2Fetcher = new HashedMap();

    public static MultiThreadConsumerManager instance(){
        return new MultiThreadConsumerManager();
    }

    private void checkAppName(String appName){
        if (name2Fetcher.containsKey(appName)){
            throw new IllegalStateException("Manager has same app name");
        }
    }

    /**
     * 该方法不会自动启动MessageFetcher线程
     * 启动操作由使用者完成
     * @param appName
     * @param properties
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> MessageFetcher<K, V> registerConsumer(String appName,
                                                        Properties properties){
        checkAppName(appName);
        return new MessageFetcher<K, V>(properties);
    }

    /**
     * 该方法会自动启动MessageFetcher线程
     * @param appName
     * @param properties
     * @param topics
     * @param listener
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> MessageFetcher<K, V> registerConsumer(String appName,
                                                        Properties properties,
                                                        Collection<String> topics,
                                                        ConsumerRebalanceListener listener,
                                                        Map<String, MessageHandler> topic2Handler,
                                                        Map<String, CommitStrategy> topic2CommitStrategy){
        checkAppName(appName);
        MessageFetcher<K, V> messageFetcher = new MessageFetcher<>(properties);
        messageFetcher.subscribe(topics, listener);
        startConsume(messageFetcher);
        messageFetcher.registerHandlers(topic2Handler);
        messageFetcher.registerCommitStrategies(topic2CommitStrategy);
        return  messageFetcher;
    }

    /**
     * 该方法会自动启动MessageFetcher线程
     * @param appName
     * @param properties
     * @param topics
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> MessageFetcher<K, V> registerConsumer(String appName,
                                                        Properties properties,
                                                        Collection<String> topics,
                                                        Map<String, MessageHandler> topic2Handler,
                                                        Map<String, CommitStrategy> topic2CommitStrategy){
        checkAppName(appName);
        MessageFetcher<K, V> messageFetcher = new MessageFetcher<>(properties);
        messageFetcher.subscribe(topics);
        startConsume(messageFetcher);
        messageFetcher.registerHandlers(topic2Handler);
        messageFetcher.registerCommitStrategies(topic2CommitStrategy);
        return  messageFetcher;
    }

    /**
     * 该方法会自动启动MessageFetcher线程
     * @param appName
     * @param properties
     * @param pattern
     * @param listener
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> MessageFetcher<K, V> registerConsumer(String appName,
                                                        Properties properties,
                                                        Pattern pattern,
                                                        ConsumerRebalanceListener listener,
                                                        Map<String, MessageHandler> topic2Handler,
                                                        Map<String, CommitStrategy> topic2CommitStrategy) {
        checkAppName(appName);
        MessageFetcher<K, V> messageFetcher = new MessageFetcher<>(properties);
        messageFetcher.subscribe(pattern, listener);
        startConsume(messageFetcher);
        messageFetcher.registerHandlers(topic2Handler);
        messageFetcher.registerCommitStrategies(topic2CommitStrategy);
        return  messageFetcher;
    }

    /**
     * 启动MessageFetcher线程
     * @param target
     */
    public void startConsume(MessageFetcher target){
        new Thread(target, "consumer[" + StrUtil.topicPartitionsStr(target.assignment()) + "] fetcher thread").start();
    }

    public void stopConsuerAsync(String appName){
        MessageFetcher messageFetcher = name2Fetcher.get(appName);
        if(messageFetcher != null){
            messageFetcher.close();
        }
        else{
            throw new IllegalStateException("manager does not have MessageFetcher named \"" + appName + "\"");
        }
    }

    public void stopConsuerSync(String appName){
        MessageFetcher messageFetcher = name2Fetcher.get(appName);
        if(messageFetcher != null){
            messageFetcher.close();
            while(!messageFetcher.isTerminated()){
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        else{
            throw new IllegalStateException("manager does not have MessageFetcher named \"" + appName + "\"");
        }
    }
}
