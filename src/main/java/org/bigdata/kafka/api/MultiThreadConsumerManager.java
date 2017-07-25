package org.bigdata.kafka.api;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.bigdata.kafka.multithread.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by hjq on 2017/6/19.
 *
 * kafka多线程工具的对外API
 *
 * 相对而言,小部分实例都是长期存在的,大部分实例属于新生代(kafka的消费实例,因为很多,所以占据大部分,以致核心对象实例只占据一小部分)
 * 1.可考虑增加新生代(尤其是Eden)的大小来减少Full GC的消耗
 * 2.或者减少fetch消息的数量,减少大量未能及时处理的消息积压在Consumer端
 *
 * 经过不严谨测试,性能OPMT2>OPMT>OPOT.
 * 消息处理时间越短,OPOT多实例模式会更有优势.
 */
public class MultiThreadConsumerManager {
    private static Logger log = LoggerFactory.getLogger(MultiThreadConsumerManager.class);
    private static final MultiThreadConsumerManager manager = new MultiThreadConsumerManager();
    private static Map<String, MessageFetcher> name2Fetcher = new HashedMap();

    public static MultiThreadConsumerManager instance(){
        return manager;
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                for(MessageFetcher messageFetcher: name2Fetcher.values()){
                    doStop(messageFetcher);
                }
            }
        }));
    }

    private MultiThreadConsumerManager() {
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
        MessageFetcher<K, V> instance = new MessageFetcher<K, V>(properties);
        name2Fetcher.put(appName, instance);
        return instance;
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
                                                        Map<String, Class<? extends MessageHandler>> topic2HandlerClass,
                                                        Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass){
        checkAppName(appName);
        MessageFetcher<K, V> messageFetcher = new MessageFetcher<>(properties);
        if(listener != null){
            messageFetcher.subscribe(topics, listener);
        }
        else{
            messageFetcher.subscribe(topics, messageFetcher.new InMemoryRebalanceListsener());
        }

        if(topic2HandlerClass == null){
            log.info("message handler not set, use default");
            topic2HandlerClass = new HashMap<>();
            for(String topic: topics){
                topic2HandlerClass.put(topic, DefaultMessageHandler.class);
            }
        }

        if(topic2CommitStrategyClass == null){
            log.info("commit strategy not set, use default");
            topic2CommitStrategyClass = new HashMap<>();
            for(String topic: topics){
                topic2CommitStrategyClass.put(topic, DefaultCommitStrategy.class);
            }
        }
        messageFetcher.registerHandlers(topic2HandlerClass);
        messageFetcher.registerCommitStrategies(topic2CommitStrategyClass);

        name2Fetcher.put(appName, messageFetcher);
        startConsume(messageFetcher);
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
                                                        Map<String, Class<? extends MessageHandler>> topic2HandlerClass,
                                                        Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass){
        checkAppName(appName);
        MessageFetcher<K, V> messageFetcher = new MessageFetcher<>(properties);
        messageFetcher.subscribe(topics);

        if(topic2HandlerClass == null){
            log.info("message handler not set, use default");
            topic2HandlerClass = new HashMap<>();
            for(String topic: topics){
                topic2HandlerClass.put(topic, DefaultMessageHandler.class);
            }
        }

        if(topic2CommitStrategyClass == null){
            log.info("commit strategy not set, use default");
            topic2CommitStrategyClass = new HashMap<>();
            for(String topic: topics){
                topic2CommitStrategyClass.put(topic, DefaultCommitStrategy.class);
            }
        }
        messageFetcher.registerHandlers(topic2HandlerClass);
        messageFetcher.registerCommitStrategies(topic2CommitStrategyClass);

        name2Fetcher.put(appName, messageFetcher);
        startConsume(messageFetcher);
        return  messageFetcher;
    }

    /**
     * 启动MessageFetcher线程
     * @param target
     */
    public void startConsume(MessageFetcher target){
        target.start();
        log.info("start consumer fetcher thread");
    }

    public void stopConsuerAsync(String appName){
        MessageFetcher messageFetcher = name2Fetcher.get(appName);
        if(messageFetcher != null){
            doStop(messageFetcher);
            name2Fetcher.remove(appName);
        }
        else{
            throw new IllegalStateException("manager does not have MessageFetcher named \"" + appName + "\"");
        }
    }

    public void stopConsumerSync(String appName){
        MessageFetcher messageFetcher = name2Fetcher.get(appName);
        if(messageFetcher != null){
            doStop(messageFetcher);
            while(!messageFetcher.isTerminated()){
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            name2Fetcher.remove(appName);
        }
        else{
            throw new IllegalStateException("manager does not have MessageFetcher named \"" + appName + "\"");
        }
    }

    private static void doStop(MessageFetcher messageFetcher){
        messageFetcher.close();
    }
}
