package org.kin.kafka.multithread.configcenter;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;
import org.kin.kafka.multithread.protocol.configcenter.DiamondMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.ConfigUtils;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hjq on 2017/6/21.
 */
public abstract class ConfigFetcher extends Thread{
    private boolean isStopped = false;
    //用于防止同时两个配置在生效,导致组件同时配置两个配置而导致最后配置信息不一致,不完整
    //异步触发应用重构配置
    private LinkedBlockingQueue<Properties> configQueue;
    //默认每3秒扫描一次
    private long fetcherInterval = 3 * 1000;
    private DiamondMasterProtocol diamondMasterProtocol;

    //节点应用信息
    private final String appName;
    private final String appHost;

    public ConfigFetcher(String host, int port, LinkedBlockingQueue<Properties> configQueue, String appName, String appHost) {
        super("ConfigFetcher");
        //创建与配置中心的RPC接口
        this.diamondMasterProtocol = RPCFactories.clientWithoutRegistry(DiamondMasterProtocol.class, host, port);
        this.configQueue = configQueue;

        this.appName = appName;
        this.appHost = appHost;
    }

    @Override
    public void run() {
        Properties lastConfig = null;
        while(!isStopped && !isInterrupted()){
            Properties newConfig = diamondMasterProtocol.getAppConfig(new ApplicationHost());
            //填充默认值
            ConfigUtils.checkRequireConfig(newConfig);
            ConfigUtils.fillDefaultConfig(newConfig);
            //如果配置更新,插队,准备更新运行时配置
            if(isChange(lastConfig, newConfig)){
                configQueue.offer(newConfig);
                //如果config fetcher interval改变
                if(!newConfig.get(AppConfig.CONFIGFETCHER_FETCHERINTERVAL).equals(lastConfig.get(AppConfig.CONFIGFETCHER_FETCHERINTERVAL))){
                    fetcherInterval = Long.valueOf(newConfig.get(AppConfig.CONFIGFETCHER_FETCHERINTERVAL).toString());
                }
                lastConfig = newConfig;
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

}
