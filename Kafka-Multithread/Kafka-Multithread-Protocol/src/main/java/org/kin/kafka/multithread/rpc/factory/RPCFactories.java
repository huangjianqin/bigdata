package org.kin.kafka.multithread.rpc.factory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.log4j.Level;
import org.kin.framework.log.LoggerBinder;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.kafka.multithread.rpc.factory.impl.DefaultRPCFactoryImpl;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Created by huangjianqin on 2017/9/8.
 */
public class RPCFactories{
    static {
        /**
         * 如果没有适合的logger使用api创建默认logger
         */
        String logger = "com.alibaba.dubbo";
        if(!LoggerBinder.exist(logger)){
            String appender = "dubbo";
            LoggerBinder.create()
                    .setLogger(Level.INFO, logger, appender)
                    .setDailyRollingFileAppender(appender)
                    .setFile(appender, "/tmp/kafka-multithread/protocol/dubbo.log")
                    .setDatePattern(appender)
                    .setAppend(appender, true)
                    .setThreshold(appender, Level.INFO)
                    .setPatternLayout(appender)
                    .setConversionPattern(appender)
                    .bind();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RPCFactories.class);
    private static final String DEFAULT_RPCFACTORY = DefaultRPCFactoryImpl.class.getName();
    private static Cache<String, RPCFactory> rpcFactories = CacheBuilder.newBuilder().softValues().build();

    private RPCFactories(){
    }

    public static RPCFactory instance(){
        return instance(DEFAULT_RPCFACTORY);
    }

    public static RPCFactory instance(final String factoryClass){
        RPCFactory rpcFactory = null;
        try {
            rpcFactory =  rpcFactories.get(factoryClass, new Callable<RPCFactory>() {
                @Override
                public RPCFactory call() throws Exception {
                    String iFactoryClass = factoryClass;
                    if(iFactoryClass == null || iFactoryClass.equals("")){
                        iFactoryClass = DEFAULT_RPCFACTORY;
                    }
                    log.info("init RPCFactory(class = " + iFactoryClass + ")");
                    RPCFactory factory = (RPCFactory) ClassUtils.instance(iFactoryClass);

                    rpcFactories.put(iFactoryClass, factory);
                    return factory;
                }
            });
        } catch (ExecutionException e) {
            log.error("hit exception when initting RPCFactory(class = " + factoryClass + ")");
            ExceptionUtils.log(e);
        }
        finally {
            return rpcFactory;
        }
    }
}
