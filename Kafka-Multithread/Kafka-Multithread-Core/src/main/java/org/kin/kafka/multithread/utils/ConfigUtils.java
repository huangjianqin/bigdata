package org.kin.kafka.multithread.utils;

import org.kin.kafka.multithread.config.AppConfig;
import scala.App;

import java.util.Properties;

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

        if(config.get(AppConfig.APPNAME) == null || config.get(AppConfig.APPNAME).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPNAME + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.APPHOST)){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPHOST + "\" is required");
        }

        if(config.get(AppConfig.APPHOST) == null || config.get(AppConfig.APPHOST).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPHOST + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.APPSTATUS)){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPSTATUS + "\" is required");
        }

        if(config.get(AppConfig.APPSTATUS) == null || config.get(AppConfig.APPSTATUS).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.APPSTATUS + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.MESSAGEHANDLER_MODEL)){
            throw new IllegalArgumentException("config \"" +  AppConfig.MESSAGEHANDLER_MODEL + "\" is required");
        }

        if(config.get(AppConfig.MESSAGEHANDLER_MODEL) == null || config.get(AppConfig.MESSAGEHANDLER_MODEL).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.MESSAGEHANDLER_MODEL + "\" is required");
        }
        //----------------------------------------------
        if(!config.containsKey(AppConfig.KAFKA_CONSUMER_SUBSCRIBE)){
            throw new IllegalArgumentException("config \"" +  AppConfig.KAFKA_CONSUMER_SUBSCRIBE + "\" is required");
        }

        if(config.get(AppConfig.KAFKA_CONSUMER_SUBSCRIBE) == null || config.get(AppConfig.KAFKA_CONSUMER_SUBSCRIBE).equals("")){
            throw new IllegalArgumentException("config \"" +  AppConfig.KAFKA_CONSUMER_SUBSCRIBE + "\" is required");
        }
    }
}
