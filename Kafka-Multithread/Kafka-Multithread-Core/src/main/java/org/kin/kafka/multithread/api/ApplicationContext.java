package org.kin.kafka.multithread.api;

import org.kin.kafka.multithread.config.AppConfig;
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

    public ApplicationContext(Application application) {
        this.application = application;
        this.appName = this.application.getConfig().getProperty(AppConfig.APPNAME);
        this.appHost = this.application.getConfig().getProperty(AppConfig.APPHOST);
        this.childRunModel = ChildRunModel.getByName(this.application.getConfig().getProperty(AppConfig.APP_CHILD_RUN_MODEL));
    }

    public ApplicationContext(Application application, String appName, String appHost, ChildRunModel childRunModel) {
        this.application = application;
        this.appName = appName;
        this.appHost = appHost;
        this.childRunModel = childRunModel;
    }

    public ApplicationContext(Application application, String appName, String appHost, String childRunModel) {
        this.application = application;
        this.appName = appName;
        this.appHost = appHost;
        this.childRunModel = ChildRunModel.getByName(childRunModel);
    }

    @Override
    public void start() {
        application.start();
    }

    @Override
    public void close() {
        application.close();
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
