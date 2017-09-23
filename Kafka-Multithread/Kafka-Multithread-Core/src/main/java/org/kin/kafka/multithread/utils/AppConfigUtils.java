package org.kin.kafka.multithread.utils;

import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.AbstractConsumerRebalanceListener;
import org.kin.kafka.multithread.api.CallBack;
import org.kin.kafka.multithread.api.CommitStrategy;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.config.DefaultAppConfig;

import javax.security.auth.callback.Callback;
import java.util.*;

/**
 * Created by huangjianqin on 2017/9/12.
 */
public class AppConfigUtils {
    public static void fillDefaultConfig(Properties config){
        if(config == null){
            return;
        }
        Properties tmp = deepCopy(AppConfig.DEFAULT_APPCONFIG);
        tmp.putAll(config);
        config.clear();
        config.putAll(tmp);
    }

    public static Properties deepCopy(Properties config){
        if(config == null){
            return null;
        }

        Properties clonedProperties = new Properties();
        for(Map.Entry<Object, Object> entry: config.entrySet()){
            clonedProperties.put(new String(entry.getKey().toString()), new String(entry.getValue().toString()));
        }
        return clonedProperties;
    }

    public static void checkRequireConfig(Properties config){
        if(config == null){
            throw new IllegalArgumentException("config is null");
        }
        //----------------------------------------------
        for(String requireConfigKey: AppConfig.REQUIRE_APPCONFIGS){
            if(!config.containsKey(requireConfigKey)){
                throw new IllegalArgumentException("config \"" +  requireConfigKey + "\" is required");
            }

            if(config.getProperty(requireConfigKey) == null || config.getProperty(requireConfigKey).equals("")){
                throw new IllegalArgumentException("config \"" +  requireConfigKey + "\" is required");
            }
        }
    }

    public static boolean isConfigItemChange(Properties lastConfig, Properties newConfig, Object key){
        if(lastConfig == null && newConfig == null){
            throw new IllegalStateException("last properties or new properties state wrong");
        }

        if(lastConfig == null && newConfig != null){
            return true;
        }

        if(lastConfig != null && newConfig == null){
            return false;
        }

        if(lastConfig.containsKey(key) && newConfig.containsKey(key)){
            if(!lastConfig.get(key).equals(newConfig.get(key))){
                return true;
            }
            else{
                return false;
            }
        }
        else{
            throw new IllegalStateException("last properties or new properties state wrong");
        }
    }

    public static boolean isConfigItemChange(Object lastValue, Properties newConfig, Object key){
        if(newConfig.containsKey(key)){
            if(!lastValue.equals(newConfig.get(key))){
                return true;
            }
            else{
                return false;
            }
        }
        else{
            throw new IllegalStateException("new properties state wrong");
        }
    }

    public static Set<String> getSubscribeTopic(Properties config){
        if(isStaticSubscribe(config)){
           throw new IllegalStateException("topic(and partition) format is not right");
        }

        Set<String> topics = new HashSet<>();
        for(String topic: config.getProperty(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).split(",")){
            topics.add(topic);
        }
        return topics;
    }

    public static Set<TopicPartition> getAssignTopicPartition(Properties config){
        if(!isStaticSubscribe(config)){
            throw new IllegalStateException("topic(and partition) format is not right");
        }

        Set<TopicPartition> topicPartitions = new HashSet<>();
        for(String topicPartition: config.getProperty(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).split(",")){
            String topic = topicPartition.split("-")[0];
            int partition = Integer.valueOf(topicPartition.split("-")[1]);
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        return topicPartitions;
    }

    public static Class<? extends MessageHandler> getMessageHandlerClass(Properties config){
        try {
            Class claxx = Class.forName(config.getOrDefault(AppConfig.MESSAGEHANDLER, DefaultAppConfig.DEFAULT_MESSAGEHANDLER).toString());
            if(claxx.isAssignableFrom(MessageHandler.class)){
                return claxx;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("message handler class wrong");
    }

    public static Class<? extends CommitStrategy> getCommitStrategyClass(Properties config){
        try {
            Class claxx = Class.forName(config.getOrDefault(AppConfig.COMMITSTRATEGY, DefaultAppConfig.DEFAULT_COMMITSTRATEGY).toString());
            if(claxx.isAssignableFrom(CommitStrategy.class)){
                return claxx;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("message handler class wrong");
    }

    public static Class<? extends AbstractConsumerRebalanceListener> getConsumerRebalanceListenerClass(Properties config){
        try {
            Class claxx = Class.forName(config.getOrDefault(AppConfig.CONSUMERREBALANCELISTENER, DefaultAppConfig.DEFAULT_CONSUMERREBALANCELISTENER).toString());
            if(claxx.isAssignableFrom(AbstractConsumerRebalanceListener.class)){
                return claxx;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("message handler class wrong");
    }

    public static Class<? extends CallBack> getCallbackClass(Properties config){
        try {
            Class claxx = Class.forName(config.getOrDefault(AppConfig.MESSAGEFETCHER_CONSUME_CALLBACK, DefaultAppConfig.DEFAULT_MESSAGEFETCHER_CONSUME_CALLBACK).toString());
            if(claxx.isAssignableFrom(Callback.class)){
                return claxx;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("message handler class wrong");
    }

    public static boolean isStaticSubscribe(Properties config){
        if(!config.containsKey(AppConfig.KAFKA_CONSUMER_SUBSCRIBE)){
            throw new IllegalStateException("topic(and partition) is not set up");
        }

        return config.getProperty(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).split(",")[0].split("-").length == 2;
    }

    public static List<Properties> allNecessaryCheckAndFill(List<Properties> newConfigs){
        List<Properties> result = new ArrayList<>();
        for(Properties config: newConfigs){
            oneNecessaryCheckAndFill(config);
            if(config != null){
                result.add(config);
            }
        }
        return result;
    }

    public static void oneNecessaryCheckAndFill(Properties newConfig){
        //检查必要配置
        AppConfigUtils.checkRequireConfig(newConfig);
        //检查配置格式
        if(!checkConfigValueFormat(newConfig)){
            return;
        }
        //填充默认值
        AppConfigUtils.fillDefaultConfig(newConfig);
    }

    public static boolean checkConfigValueFormat(Properties config){
        for(Map.Entry<String, String> entry: AppConfig.CONFIG2FORMATOR.entrySet()){
            if(!config.getProperty(entry.getKey()).matches(entry.getValue())){
                throw new IllegalStateException("config \"" +  entry.getKey() + "\" 's value \"" + entry.getValue() + "\" format is not correct");
            }
        }
        return true;
    }
}
