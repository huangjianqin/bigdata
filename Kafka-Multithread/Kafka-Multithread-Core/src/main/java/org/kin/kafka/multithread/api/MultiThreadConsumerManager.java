package org.kin.kafka.multithread.api;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ReConfigable;
import org.kin.kafka.multithread.core.AbstractMessageHandlersManager;
import org.kin.kafka.multithread.core.Application;
import org.kin.kafka.multithread.core.MessageFetcher;
import org.kin.kafka.multithread.core.OCOTMultiProcessor;
import org.kin.kafka.multithread.distributed.ChildRunModel;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
public class MultiThreadConsumerManager implements ReConfigable{
    private static final Logger log = LoggerFactory.getLogger(MultiThreadConsumerManager.class);
    private static final MultiThreadConsumerManager manager = new MultiThreadConsumerManager();
    private static final Map<String, ApplicationContext> appName2ApplicationContext = new HashMap();

    public static MultiThreadConsumerManager instance(){
        return manager;
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                for(ApplicationContext applicationContext: appName2ApplicationContext.values()){
                    applicationContext.close();
                }
                appName2ApplicationContext.clear();
            }
        }));
    }

    private MultiThreadConsumerManager() {
    }

    private void checkAppName(String appName){
        if (appName2ApplicationContext.containsKey(appName)){
            throw new IllegalStateException("Manager has same app name");
        }
    }

    public <K, V> ApplicationContext newApplication(Properties config){
        AppConfigUtils.oneNecessaryCheckAndFill(config);

        String appName = config.getProperty(AppConfig.APPNAME);

        checkAppName(appName);

        String appHost = config.getProperty(AppConfig.APPHOST);
        ChildRunModel childRunModel = ChildRunModel.getByName(config.getProperty(AppConfig.APP_CHILD_RUN_MODEL));
        AbstractMessageHandlersManager.MsgHandlerManagerModel msgHandlerModel = AbstractMessageHandlersManager.MsgHandlerManagerModel.getByDesc(
                config.getProperty(AppConfig.MESSAGEHANDLERMANAGER_MODEL)
        );

        Application application = null;
        switch (msgHandlerModel){
            case OPOT:
            case OPMT:
            case OPMT2:
                MessageFetcher<K, V> messageFetcher = new MessageFetcher<>(config);
                application = messageFetcher;
                break;
            case OCOT:
                OCOTMultiProcessor<K, V> processor = new OCOTMultiProcessor<>(config);
                application = processor;
                break;
            default:
                throw new IllegalStateException("something wrong");
        }

        if(application != null){
            ApplicationContext applicationContext = new ApplicationContext(
                    application,
                    appName,
                    appHost,
                    childRunModel
            );
            appName2ApplicationContext.put(appName, applicationContext);
            return applicationContext;
        }
        else{
            throw new IllegalStateException("init application error");
        }
    }

    public ApplicationContext getApplicationContext(String appName){
        if(appName2ApplicationContext.containsKey(appName)){
            return appName2ApplicationContext.get(appName);
        }
        return null;
    }

    public int getAppSize(){
        return appName2ApplicationContext.size();
    }

    public boolean containsAppName(String appName){
        return appName2ApplicationContext.containsKey(appName);
    }

    public void shutdownApp(String appName){
        appName2ApplicationContext.get(appName).close();
        appName2ApplicationContext.remove(appName);
    }

    @Override
    public void reConfig(Properties newConfig) {
        AppConfigUtils.oneNecessaryCheckAndFill(newConfig);

        String appName = newConfig.getProperty(AppConfig.APPNAME);

        checkAppName(appName);

        appName2ApplicationContext.get(appName).reConfig(newConfig);
    }
}
