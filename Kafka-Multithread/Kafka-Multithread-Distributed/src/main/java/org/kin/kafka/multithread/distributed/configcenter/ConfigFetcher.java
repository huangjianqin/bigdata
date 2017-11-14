package org.kin.kafka.multithread.distributed.configcenter;

import org.apache.log4j.Level;
import org.kin.framework.log.Log4jLoggerBinder;
import org.kin.kafka.multithread.utils.ExceptionUtils;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.distributed.AppStatus;
import org.kin.kafka.multithread.distributed.node.config.DefaultNodeConfig;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatResponse;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatRequest;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.protocol.configcenter.DiamondMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.utils.HostUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by hjq on 2017/6/21.
 * 定时fetch配置
 * 在其他线程对Application进行更新配置
 */
public class ConfigFetcher extends Thread{
    static {log();}
    private static final Logger log = LoggerFactory.getLogger("ConfigFetcher");

    private boolean isStopped = false;
    //用于防止同时两个配置在生效,导致组件同时配置两个配置而导致最后配置信息不一致,不完整
    //异步触发应用重构配置
    private LinkedBlockingDeque<Properties> configQueue;
    //默认每3秒扫描一次
    private long heartbeatInterval;

    private DiamondContext diamondContext;
    private DiamondMasterProtocol diamondMasterProtocol;
    private int fetcherRestartTimes = 0;

    private List<ApplicationContextInfo> succeedAppNames = new ArrayList<>();
    private List<ApplicationContextInfo> failAppNames = new ArrayList<>();

    public ConfigFetcher(LinkedBlockingDeque<Properties> configQueue) {
        super("ConfigFetcher");
        log();
        //创建与配置中心的RPC接口
        String host = DefaultNodeConfig.DEFAULT_CONFIGCENTER_HOST;
        int port = Integer.valueOf(DefaultNodeConfig.DEFAULT_CONFIGCENTER_PORT);
        heartbeatInterval = Integer.valueOf(DefaultNodeConfig.DEFAULT_CONFIGFETCHER_HEARTBEAT);
        this.diamondContext = new DiamondContext(host, port);
        this.configQueue = configQueue;
        log.info("ready to fetch config from config center(" + host + ":" + port + ")");
    }

    public ConfigFetcher(String host, int port, LinkedBlockingDeque<Properties> configQueue, int heartbeatInterval) {
        super("ConfigFetcher");
        log();
        this.heartbeatInterval = heartbeatInterval;
        //创建与配置中心的RPC接口
        this.diamondContext = new DiamondContext(host, port);
        this.configQueue = configQueue;
        log.info("ready to fetch config from config center(" + host + ":" + port + ")");
    }

    /**
     * 如果没有适合的logger使用api创建默认logger
     */
    private static void log(){
        String logger = "ConfigFetcher";
        if(!Log4jLoggerBinder.exist(logger)){
            String appender = "configfetcher";
            Log4jLoggerBinder.create()
                    .setLogger(Level.INFO, logger, appender)
                    .setDailyRollingFileAppender(appender)
                    .setFile(appender, "/tmp/kafka-multithread/distributed/configfetcher.log")
                    .setDatePattern(appender)
                    .setAppend(appender, true)
                    .setThreshold(appender, Level.INFO)
                    .setPatternLayout(appender)
                    .setConversionPattern(appender)
                    .bind();
        }
    }

    private void initDiamondClient(DiamondContext diamondContext){
        this.diamondMasterProtocol =
                RPCFactories.clientWithoutRegistry(
                        DiamondMasterProtocol.class,
                        diamondContext.getHost(),
                        diamondContext.getPort()
                );
    }

    @Override
    public void run() {
        ApplicationContextInfo myAppHost = new ApplicationContextInfo();
        myAppHost.setHost(HostUtils.localhost());

        int configCount = 0;
        int fetchTimeMills = 0;
        try{
            log.info("init diamond client");
            initDiamondClient(this.diamondContext);

            log.info("start to fetch config");
            while(!isStopped && !isInterrupted()){
                ConfigFetcherHeartbeatResponse configFetcherHeartbeatResponse = heartbeat();

                log.debug("fetch " + configFetcherHeartbeatResponse.getNewConfigs().size() + " configs at " + configFetcherHeartbeatResponse.getResponseTime());
                configCount += configFetcherHeartbeatResponse.getNewConfigs().size();
                if(fetchTimeMills >= TimeUnit.MINUTES.toMillis(1)){
                    fetchTimeMills = 0;
                    configCount = 0;
                    log.info("fetch {} configs /min", configCount);
                }

                long startTime = System.currentTimeMillis();
                //配置内容,格式匹配成功的配置
                List<Properties> halfSuccessConfigs = AppConfigUtils.allNecessaryCheckAndFill(configFetcherHeartbeatResponse.getNewConfigs());

                for(Properties newConfig: halfSuccessConfigs){
                    String appName = newConfig.getProperty(AppConfig.APPNAME);
                    log.info(appName + " is a new app, ready to config and run");
                    configQueue.offer(newConfig);
                }

                long endTime = System.currentTimeMillis();

                try {
                    fetchTimeMills += (endTime - startTime);
                    if(endTime - startTime < heartbeatInterval){
                        sleep(heartbeatInterval - (endTime - startTime));
                    }
                } catch (InterruptedException e) {
                    ExceptionUtils.log(e);
                }
            }
            log.info("config fetcher closed");
        }catch (Exception e){
            ExceptionUtils.log(e);
            if(fetcherRestartTimes < 3){
                fetcherRestartTimes ++;
                log.warn("config Fetcher hit error when running, ready to restart(only has {} times chance)", 3 - fetcherRestartTimes);
                run();
            }
            else{
                close();
                log.info("config fetcher closed because of error");
                //ConfigFetcher报错,导致Node异常退出,意味着无法连接Diamond,同时无法上报该Node拥有的所有Application
                //...
                System.exit(-1);
            }
        }
    }

    public void close(){
        log.info("config fetcher closing...");
        isStopped = true;
    }

    private ConfigFetcherHeartbeatResponse heartbeat(){
        ApplicationContextInfo appHost = new ApplicationContextInfo("", HostUtils.localhost());

        List<ApplicationContextInfo> tmpSucceedAppNames = new ArrayList<>(succeedAppNames);
        List<ApplicationContextInfo> tmpFailAppNames = new ArrayList<>(failAppNames);

        succeedAppNames.removeAll(tmpSucceedAppNames);
        failAppNames.removeAll(tmpFailAppNames);

        ConfigFetcherHeartbeatRequest heartbeat = new ConfigFetcherHeartbeatRequest(
                appHost,
                tmpSucceedAppNames,
                tmpFailAppNames,
                System.currentTimeMillis());
        return diamondMasterProtocol.heartbeat(heartbeat);
    }

    public void appFail(List<ApplicationContextInfo> failApplicationContextInfos){
        for(ApplicationContextInfo failApplicationContextInfo: failApplicationContextInfos){
            log.info(failApplicationContextInfo.getAppName() + " config set up fail");
        }
        this.failAppNames.addAll(failApplicationContextInfos);
        log.info(failApplicationContextInfos.size() + " configs set up fail");
    }

    public void configFail(List<Properties> failConfigs){
        List<ApplicationContextInfo> failAppNames = new ArrayList<>();
        for(Properties config: failConfigs){
            ApplicationContextInfo failApplicationContextInfo = new ApplicationContextInfo();
            failApplicationContextInfo.setAppName(config.getProperty(AppConfig.APPNAME));
            failApplicationContextInfo.setHost(config.getProperty(AppConfig.APPHOST));
            failAppNames.add(failApplicationContextInfo);
            log.info(failApplicationContextInfo.getAppName() + " config set up fail");
        }
        this.failAppNames.addAll(failAppNames);
        log.info(failAppNames.size() + " configs set up fail");
    }

    private void updateAppStatus(ApplicationContextInfo applicationContextInfo, AppStatus expectedAppStatus){
        switch (expectedAppStatus){
            case RUN:
            case UPDATE:
            case RESTART:
                applicationContextInfo.setAppStatus(AppStatus.RUN);
                break;
            case CLOSE:
                applicationContextInfo.setAppStatus(AppStatus.CLOSE);
                break;
        }
    }

    public void appSucceed(List<ApplicationContextInfo> succeedApplicationContextInfos){
        for(ApplicationContextInfo succeedApplicationContextInfo: succeedApplicationContextInfos){
            AppStatus expectedAppStatus = succeedApplicationContextInfo.getAppStatus();
            updateAppStatus(succeedApplicationContextInfo, expectedAppStatus);
            log.info(succeedApplicationContextInfo.getAppName() + " config set up succeed");
        }
        this.succeedAppNames.addAll(succeedApplicationContextInfos);
        log.info(succeedApplicationContextInfos.size() + " configs set up succeed");
    }

    public void configSucceed(List<Properties> succeedConfigs){
        List<ApplicationContextInfo> succeedAppNames = new ArrayList<>();
        for(Properties config: succeedConfigs){
            ApplicationContextInfo succeedApplicationContextInfo = new ApplicationContextInfo();

            succeedApplicationContextInfo.setAppName(config.getProperty(AppConfig.APPNAME));
            succeedApplicationContextInfo.setHost(config.getProperty(AppConfig.APPHOST));

            AppStatus expectedAppStatus = AppStatus.getByStatusDesc(config.getProperty(AppConfig.APPSTATUS));
            updateAppStatus(succeedApplicationContextInfo, expectedAppStatus);

            succeedAppNames.add(succeedApplicationContextInfo);
            log.info(succeedApplicationContextInfo.getAppName() + " config set up succeed");
        }
        this.succeedAppNames.addAll(succeedAppNames);
        log.info(succeedAppNames.size() + " configs set up succeed");
    }
}
