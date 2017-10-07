package org.kin.kafka.multithread.api;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.core.AbstractMessageHandlersManager;
import org.kin.kafka.multithread.core.Application;
import org.kin.kafka.multithread.distributed.ChildRunModel;

import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/18.
 */
public class ApplicationContext implements Application {
    private Application application;
    private String appName;
    private String appHost;
    private ChildRunModel childRunModel;
    private AbstractMessageHandlersManager.MsgHandlerManagerModel msgHandlerManagerModel;
    private MultiThreadConsumerManager manager;
    private boolean isClosed = false;

    public ApplicationContext(Application application, MultiThreadConsumerManager manager) {
        this.application = application;
        this.appName = this.application.getConfig().getProperty(AppConfig.APPNAME);
        this.appHost = this.application.getConfig().getProperty(AppConfig.APPHOST);
        this.msgHandlerManagerModel = AbstractMessageHandlersManager.MsgHandlerManagerModel.getByDesc(this.application.getConfig().getProperty(AppConfig.MESSAGEHANDLERMANAGER_MODEL));
        this.childRunModel = ChildRunModel.getByName(this.application.getConfig().getProperty(AppConfig.APP_CHILD_RUN_MODEL));
        this.manager = manager;
    }

    public ApplicationContext(
            Application application,
            String appName,
            String appHost,
            AbstractMessageHandlersManager.MsgHandlerManagerModel msgHandlerManagerModel,
            ChildRunModel childRunModel,
            MultiThreadConsumerManager manager) {
        this.application = application;
        this.appName = appName;
        this.appHost = appHost;
        this.msgHandlerManagerModel = msgHandlerManagerModel;
        this.childRunModel = childRunModel;
        this.manager = manager;
    }

    public ApplicationContext(
            Application application,
            String appName,
            String appHost,
            String childRunModelStr,
            String msgHandlerManagerModelStr,
            MultiThreadConsumerManager manager) {
        this.application = application;
        this.appName = appName;
        this.appHost = appHost;
        this.msgHandlerManagerModel = AbstractMessageHandlersManager.MsgHandlerManagerModel.getByDesc(msgHandlerManagerModelStr);
        this.childRunModel = ChildRunModel.getByName(childRunModelStr);
        this.manager = manager;
    }

    @Override
    public void start() {
        application.start();
    }

    @Override
    public void close() {
        if(isClosed){
            return;
        }
        isClosed = true;
        application.close();
        manager.shutdownApp(appName);
    }

    @Override
    public Properties getConfig() {
        return application.getConfig();
    }

    @Override
    public void reConfig(Properties newConfig) {
        application.reConfig(newConfig);
    }


    public String getAppName() {
        return appName;
    }

    public String getAppHost() {
        return appHost;
    }

    public ChildRunModel getChildRunModel() {
        return childRunModel;
    }
}
