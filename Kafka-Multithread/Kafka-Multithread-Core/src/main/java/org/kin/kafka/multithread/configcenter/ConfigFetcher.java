package org.kin.kafka.multithread.configcenter;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.config.DefaultAppConfig;
import org.kin.kafka.multithread.domain.ConfigFetchResult;
import org.kin.kafka.multithread.domain.ConfigSetupResult;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;
import org.kin.kafka.multithread.protocol.configcenter.DiamondMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.utils.HostUtils;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hjq on 2017/6/21.
 * 定时fetch配置
 * 在其他线程对Application进行更新配置
 */
public class ConfigFetcher extends Thread{
    private boolean isStopped = false;
    //用于防止同时两个配置在生效,导致组件同时配置两个配置而导致最后配置信息不一致,不完整
    //异步触发应用重构配置
    private LinkedBlockingQueue<Properties> configQueue;
    //默认每3秒扫描一次
    private long fetcherInterval = 3 * 1000;
    private DiamondMasterProtocol diamondMasterProtocol;

    public ConfigFetcher(LinkedBlockingQueue<Properties> configQueue) {
        super("ConfigFetcher");
        //创建与配置中心的RPC接口
        String host = DefaultAppConfig.DEFAULT_CONFIGCENTER_HOST;
        int port = Integer.valueOf(DefaultAppConfig.DEFAULT_CONFIGCENTER_PORT);
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;
    }

    public ConfigFetcher(String host, int port, LinkedBlockingQueue<Properties> configQueue) {
        super("ConfigFetcher");
        //创建与配置中心的RPC接口
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;
    }

    public long getFetcherInterval() {
        return fetcherInterval;
    }

    public void setFetcherInterval(long fetcherInterval) {
        this.fetcherInterval = fetcherInterval;
    }

    @Override
    public void run() {
        Map<String, Properties> app2Config = null;
        ApplicationHost myAppHost = new ApplicationHost();
        myAppHost.setHost(HostUtils.localhost());
        while(!isStopped && !isInterrupted()){
            ConfigFetchResult configFetchResult = diamondMasterProtocol.getAppConfig(myAppHost);

            //配置内容,格式匹配成功的配置
            List<Properties> halfSuccessConfigs = AppConfigUtils.allNecessaryCheckAndFill(configFetchResult.getNewConfigs());
            List<Properties> failConfigs = configFetchResult.getNewConfigs();
            failConfigs.removeAll(halfSuccessConfigs);

            //通知config center配置更新失败,回滚以前的配置
            if(failConfigs.size() > 0){
                List<String> failAppNames = new ArrayList<>();
                for(Properties config: failConfigs){
                    failAppNames.add(config.getProperty(AppConfig.APPNAME));
                }
                ConfigSetupResult result = new ConfigSetupResult(failAppNames, System.currentTimeMillis());
                diamondMasterProtocol.configFail(result);
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
                    if(isChange(lastConfig, newConfig)){
                        configQueue.offer(newConfig);
                        app2Config.put(appName, newConfig);
                    }
                }
                else{
                    configQueue.offer(newConfig);
                    app2Config.put(appName, newConfig);
                }
            }
            try {
                sleep(fetcherInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isChange(Properties lastConfig, Properties newConfig){
        if(lastConfig != null && newConfig == null){
            return false;
        }

        //刚启动
        if(lastConfig == null && newConfig != null){
            return true;
        }

        boolean result = false;
        if(lastConfig != null && newConfig != null){
            if(lastConfig.size() == newConfig.size()){
                for(Object key: lastConfig.keySet()){
                    if(newConfig.contains(key)){
                        if(lastConfig.get(key).equals(newConfig.get(key))){
                            continue;
                        }
                    }
                    result = true;
                    break;
                }
                return result;
            }
            return true;
        }
        return false;
    }

    public void close(){
        isStopped = true;
    }

    public void configFail(List<Properties> failConfigs){
        List<String> failAppNames = new ArrayList<>();
        for(Properties config: failConfigs){
            failAppNames.add(config.getProperty(AppConfig.APPNAME));
        }
        ConfigSetupResult result = new ConfigSetupResult(failAppNames, System.currentTimeMillis());
        diamondMasterProtocol.configFail(result);
    }
}
