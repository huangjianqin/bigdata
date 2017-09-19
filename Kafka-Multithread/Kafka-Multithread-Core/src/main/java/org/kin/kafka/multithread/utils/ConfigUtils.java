package org.kin.kafka.multithread.utils;

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
public class ConfigUtils {
    public static void fillDefaultConfig(Properties config){
        if(config == null){
            return;
        }
        Properties defaultConfig = AppConfig.DEFAULT_APPCONFIG;
        for(Object key: defaultConfig.keySet()){
            if(config.containsKey(key) && (config.get(key) == null || config.get(key).equals(""))){
                config.put(key, defaultConfig.get(key));
            }
        }
    }

    public static void checkRequireConfig(Properties config){
        if(config == null){
            return;
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.APPNAME)){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPNAME + "\" is required");
        }

        if(config.getProperty(AppConfig.APPNAME) == null || config.getProperty(AppConfig.APPNAME).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPNAME + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.APPHOST)){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPHOST + "\" is required");
        }

        if(config.getProperty(AppConfig.APPHOST) == null || config.getProperty(AppConfig.APPHOST).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPHOST + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.APPSTATUS)){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPSTATUS + "\" is required");
        }

        if(config.getProperty(AppConfig.APPSTATUS) == null || config.getProperty(AppConfig.APPSTATUS).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPSTATUS + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.MESSAGEHANDLERMANAGER_MODEL)){
            throw new IllegalArgumentException("config \"" +  AppConfig.MESSAGEHANDLERMANAGER_MODEL + "\" is required");
        }

        if(config.getProperty(AppConfig.MESSAGEHANDLERMANAGER_MODEL) == null || config.getProperty(AppConfig.MESSAGEHANDLERMANAGER_MODEL).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.MESSAGEHANDLERMANAGER_MODEL + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.KAFKA_CONSUMER_SUBSCRIBE)){
            throw new IllegalArgumentException("config \"" +  AppConfig.KAFKA_CONSUMER_SUBSCRIBE + "\" is required");
        }

        if(config.getProperty(AppConfig.KAFKA_CONSUMER_SUBSCRIBE) == null || config.getProperty(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.KAFKA_CONSUMER_SUBSCRIBE + "\" is required");
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
        Set<String> topics = new HashSet<>();
        for(String topic: config.get(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).toString().split(",")){
            topics.add(topic);
        }
        return topics;
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

    public static boolean isAutoSubscribe(Properties config){
        return config.get(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).toString().split(",")[0].split("-").length == 2;
    }

    public static List<Properties> allNecessaryCheckAndFill(List<Properties> newConfigs){
        List<Properties> result = new ArrayList<>();
        for(Properties config: newConfigs){
            if(oneNecessaryCheckAndFill(config)){
                result.add(config);
            }
        }
        return result;
    }

    public static boolean oneNecessaryCheckAndFill(Properties newConfig){
        //检查必要配置
        ConfigUtils.checkRequireConfig(newConfig);
        //检查配置格式
        //...
        //填充默认值
        ConfigUtils.fillDefaultConfig(newConfig);

        return true;
    }
}
