package org.kin.kafka.multithread.configcenter.manager;

import org.apache.log4j.Level;
import org.kin.framework.log.LoggerBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangjianqin on 2017/11/2.
 */
public abstract class AbstractConfigStoreManager implements ConfigStoreManager {
    static {log();}
    protected static final Logger log = LoggerFactory.getLogger("ConfigStoreManager");

    /**
     * 如果没有适合的logger使用api创建默认logger
     */
    private static void log(){
        String logger = "ConfigStoreManager";
        if(!LoggerBinder.exist(logger)){
            String appender = "configstoremanager";
            LoggerBinder.create()
                    .setLogger(Level.INFO, logger, appender)
                    .setDailyRollingFileAppender(appender)
                    .setFile(appender, "/tmp/kafka-multithread/config/configStoreManager.log")
                    .setDatePattern(appender)
                    .setAppend(appender, true)
                    .setThreshold(appender, Level.INFO)
                    .setPatternLayout(appender)
                    .setConversionPattern(appender)
                    .bind();
        }
    }
}
