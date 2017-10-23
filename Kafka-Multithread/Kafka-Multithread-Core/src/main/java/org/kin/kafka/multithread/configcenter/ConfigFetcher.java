package org.kin.kafka.multithread.configcenter;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.config.DefaultAppConfig;
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
import java.util.concurrent.LinkedBlockingQueue;

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
    private LinkedBlockingQueue<Properties> configQueue;
    //默认每3秒扫描一次
    private long fetcherInterval = 3 * 1000;
    private DiamondMasterProtocol diamondMasterProtocol;

    //与配置中心Diamond的定时心跳,不可配置,主要用于反馈配置是否生效
    private Timer heartbeatTimer;
    private List<String> succeedAppNames = new ArrayList<>();
    private List<String> failAppNames = new ArrayList<>();

    public ConfigFetcher(LinkedBlockingQueue<Properties> configQueue) {
        super("ConfigFetcher");
        //创建与配置中心的RPC接口
        String host = DefaultAppConfig.DEFAULT_CONFIGCENTER_HOST;
        int port = Integer.valueOf(DefaultAppConfig.DEFAULT_CONFIGCENTER_PORT);
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;
        log.info("ready to fetch config from config center(" + host + ":" + port + ")");
    }

    public ConfigFetcher(String host, int port, LinkedBlockingQueue<Properties> configQueue) {
        super("ConfigFetcher");
        //创建与配置中心的RPC接口
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;
        log.info("ready to fetch config from config center(" + host + ":" + port + ")");
    }

    @Override
    public void run() {
        log.info("start to fetch config");

        //启动定时心跳
        heartbeatTimer = new Timer();
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {

            }
        }, 0, 5 * 1000);

        Map<String, Properties> app2Config = null;
        ApplicationContextInfo myAppHost = new ApplicationContextInfo();
        myAppHost.setHost(HostUtils.localhost());
        while(!isStopped && !isInterrupted()){
            ConfigFetcherHeartbeatResponse configFetcherHeartbeatResponse = heartbeat();

            log.info("fetch " + configFetcherHeartbeatResponse.getNewConfigs().size() + " configs at " + configFetcherHeartbeatResponse.getResponseTime());

            long startTime = System.currentTimeMillis();
            //配置内容,格式匹配成功的配置
            List<Properties> halfSuccessConfigs = AppConfigUtils.allNecessaryCheckAndFill(configFetcherHeartbeatResponse.getNewConfigs());
            List<Properties> failConfigs = configFetcherHeartbeatResponse.getNewConfigs();
            failConfigs.removeAll(halfSuccessConfigs);

            //通知config center配置更新失败,回滚以前的配置
            if(failConfigs.size() > 0){
                List<String> failAppNames = new ArrayList<>();
                for(Properties config: failConfigs){
                    String failAppName = config.getProperty(AppConfig.APPNAME);
                    failAppNames.add(failAppName);
                    log.info(failAppName + " config set up fail");
                }
                this.failAppNames.addAll(failAppNames);
            }

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
                if(endTime - startTime < fetcherInterval){
                    sleep(fetcherInterval - (endTime - startTime));
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

        List<String> tmpSucceedAppNames = succeedAppNames.subList(0, succeedAppNames.size());
        List<String> tmpFailAppNames = failAppNames.subList(0, succeedAppNames.size());
        succeedAppNames.removeAll(tmpSucceedAppNames);
        failAppNames.removeAll(tmpFailAppNames);
        ConfigFetcherHeartbeatRequest heartbeat = new ConfigFetcherHeartbeatRequest(
                appHost,
                tmpSucceedAppNames,
                tmpFailAppNames,
                System.currentTimeMillis());
        return diamondMasterProtocol.heartbeat(heartbeat);
    }

    public void configFailAppNames(List<String> failAppNames){
        for(String failAppName: failAppNames){
            log.info(failAppName + " config set up fail");
        }
        this.failAppNames.addAll(failAppNames);
        log.info(failAppNames.size() + " configs set up fail");
    }

    public void configFailConfigs(List<Properties> failConfigs){
        List<String> failAppNames = new ArrayList<>();
        for(Properties config: failConfigs){
            String failAppName = config.getProperty(AppConfig.APPNAME);
            failAppNames.add(failAppName);
            log.info(failAppName + " config set up fail");
        }
        this.failAppNames.addAll(failAppNames);
        log.info(failAppNames.size() + " configs set up fail");
    }

    public void configSucceedAppNames(List<String> succeedAppNames){
        for(String succeedAppName: succeedAppNames){
            log.info(succeedAppName + " config set up succeed");
        }
        this.succeedAppNames.addAll(succeedAppNames);
        log.info(succeedAppNames.size() + " configs set up succeed");
    }

    public void configSucceedConfigs(List<Properties> succeedConfigs){
        List<String> succeedAppNames = new ArrayList<>();
        for(Properties config: succeedConfigs){
            String succeedAppName = config.getProperty(AppConfig.APPNAME);
            succeedAppNames.add(succeedAppName);
            log.info(succeedAppName + " config set up succeed");
        }
        this.succeedAppNames.addAll(succeedAppNames);
        log.info(succeedAppNames.size() + " configs set up succeed");
    }
}
