package org.kin.kafka.multithread.distributed.configcenter;

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
    private static final Logger log = LoggerFactory.getLogger(ConfigFetcher.class);

    private boolean isStopped = false;
    //用于防止同时两个配置在生效,导致组件同时配置两个配置而导致最后配置信息不一致,不完整
    //异步触发应用重构配置
    private LinkedBlockingDeque<Properties> configQueue;
    //默认每3秒扫描一次
    private long heartbeatInterval;
    private DiamondMasterProtocol diamondMasterProtocol;

    private List<ApplicationContextInfo> succeedAppNames = new ArrayList<>();
    private List<ApplicationContextInfo> failAppNames = new ArrayList<>();

    public ConfigFetcher(LinkedBlockingDeque<Properties> configQueue) {
        super("ConfigFetcher");
        //创建与配置中心的RPC接口
        String host = DefaultNodeConfig.DEFAULT_CONFIGCENTER_HOST;
        int port = Integer.valueOf(DefaultNodeConfig.DEFAULT_CONFIGCENTER_PORT);
        heartbeatInterval = Integer.valueOf(DefaultNodeConfig.DEFAULT_CONFIGFETCHER_HEARTBEAT);
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;
        log.info("ready to fetch config from config center(" + host + ":" + port + ")");
    }

    public ConfigFetcher(String host, int port, LinkedBlockingDeque<Properties> configQueue, int heartbeatInterval) {
        super("ConfigFetcher");
        this.heartbeatInterval = heartbeatInterval;
        //创建与配置中心的RPC接口
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;
        log.info("ready to fetch config from config center(" + host + ":" + port + ")");
    }

    @Override
    public void run() {
        log.info("start to fetch config");

        Map<String, Properties> app2Config = null;
        ApplicationContextInfo myAppHost = new ApplicationContextInfo();
        myAppHost.setHost(HostUtils.localhost());

        int configCount = 0;
        int fetchTimeMills = 0;
        while(!isStopped && !isInterrupted()){
            ConfigFetcherHeartbeatResponse configFetcherHeartbeatResponse = heartbeat();

            log.debug("fetch " + configFetcherHeartbeatResponse.getNewConfigs().size() + " configs at " + configFetcherHeartbeatResponse.getResponseTime());
            configCount += configFetcherHeartbeatResponse.getNewConfigs().size();
            if(fetchTimeMills >= TimeUnit.MINUTES.toMillis(1)){
                fetchTimeMills = 0;
                configCount = 0;
                log.debug("fetch {} configs /min", configCount);
            }

            long startTime = System.currentTimeMillis();
            //配置内容,格式匹配成功的配置
            List<Properties> halfSuccessConfigs = AppConfigUtils.allNecessaryCheckAndFill(configFetcherHeartbeatResponse.getNewConfigs());

            Map<String, Properties> newApp2Config = new HashMap<>();
            for(Properties config: halfSuccessConfigs){
                String appName = config.getProperty(AppConfig.APPNAME);
                newApp2Config.put(appName, config);
            }

            for(String appName: newApp2Config.keySet()){
                Properties lastConfig = app2Config.get(appName);
                Properties newConfig = newApp2Config.get(appName);
                if(lastConfig != null){
                    //如果配置更新,插队,准备更新运行时配置
                    if(AppConfigUtils.isConfigChange(lastConfig, newConfig)){
                        log.info(appName + " config changed, ready to update app config");
                        configQueue.offer(newConfig);
                        app2Config.put(appName, newConfig);
                    }
                    else{
                        log.info(appName + " config not changed");
                    }
                }
                else{
                    log.info(appName + " is a new app, ready to config and run");
                    configQueue.offer(newConfig);
                    app2Config.put(appName, newConfig);
                }
            }

            long endTime = System.currentTimeMillis();

            try {
                fetchTimeMills += (endTime - startTime);
                if(endTime - startTime < heartbeatInterval){
                    sleep(heartbeatInterval - (endTime - startTime));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("config fetcher closed");
    }

    public void close(){
        log.info("config fetcher closing...");
        isStopped = true;
    }

    private ConfigFetcherHeartbeatResponse heartbeat(){
        ApplicationContextInfo appHost = new ApplicationContextInfo("", HostUtils.localhost());

        List<ApplicationContextInfo> tmpSucceedAppNames = succeedAppNames.subList(0, succeedAppNames.size());
        List<ApplicationContextInfo> tmpFailAppNames = failAppNames.subList(0, succeedAppNames.size());
        succeedAppNames.removeAll(tmpSucceedAppNames);
        failAppNames.removeAll(tmpFailAppNames);
        ConfigFetcherHeartbeatRequest heartbeat = new ConfigFetcherHeartbeatRequest(
                appHost,
                tmpSucceedAppNames,
                tmpFailAppNames,
                System.currentTimeMillis());
        return diamondMasterProtocol.heartbeat(heartbeat);
    }

    public void configFailAppNames(List<ApplicationContextInfo> failApplicationContextInfos){
        for(ApplicationContextInfo failApplicationContextInfo: failApplicationContextInfos){
            log.info(failApplicationContextInfo.getAppName() + " config set up fail");
        }
        this.failAppNames.addAll(failApplicationContextInfos);
        log.info(failApplicationContextInfos.size() + " configs set up fail");
    }

    public void configFailConfigs(List<Properties> failConfigs){
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

    public void configSucceedAppNames(List<ApplicationContextInfo> succeedApplicationContextInfos){
        for(ApplicationContextInfo succeedApplicationContextInfo: succeedApplicationContextInfos){
            log.info(succeedApplicationContextInfo.getAppName() + " config set up succeed");
        }
        this.succeedAppNames.addAll(succeedApplicationContextInfos);
        log.info(succeedApplicationContextInfos.size() + " configs set up succeed");
    }

    public void configSucceedConfigs(List<Properties> succeedConfigs){
        List<ApplicationContextInfo> succeedAppNames = new ArrayList<>();
        for(Properties config: succeedConfigs){
            ApplicationContextInfo succeedApplicationContextInfo = new ApplicationContextInfo();
            succeedApplicationContextInfo.setAppName(config.getProperty(AppConfig.APPNAME));
            succeedApplicationContextInfo.setHost(config.getProperty(AppConfig.APPHOST));
            AppStatus expectedAppStatus = AppStatus.getByStatusDesc(config.getProperty(AppConfig.APPSTATUS));
            switch (expectedAppStatus){
                case RUN:
                case UPDATE:
                case RESTART:
                    succeedApplicationContextInfo.setAppStatus(AppStatus.RUN);
                    break;
                case CLOSE:
                    succeedApplicationContextInfo.setAppStatus(AppStatus.CLOSE);
                    break;
            }
            succeedAppNames.add(succeedApplicationContextInfo);
            log.info(succeedApplicationContextInfo.getAppName() + " config set up succeed");
        }
        this.succeedAppNames.addAll(succeedAppNames);
        log.info(succeedAppNames.size() + " configs set up succeed");
    }
}
