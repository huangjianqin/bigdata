package org.kin.kafka.multithread.configcenter;

import org.kin.kafka.multithread.configcenter.protocol.ReConfigable;

import java.util.Properties;

/**
 * Created by hjq on 2017/6/21.
 */
public abstract class ConfigFetcher extends Thread {
    private boolean isStopped = false;
    private ReConfigable target;
    //默认每分钟扫描一次
    private long scanTime = 60 * 1000;

    public ConfigFetcher(ReConfigable target, long scanTime) {
        super(target.getClass().getName() + "-scan Thread(scan config sec " + scanTime + "ms)");
        this.target = target;
        this.scanTime = scanTime;
    }

    //目前想法是通过json格式定义配置
    public abstract Properties fetchConfig();

    @Override
    public void run() {
        Properties lastConfig = null;
        while(!isStopped && !isInterrupted()){
            Properties newConfig = fetchConfig();
            if(isChange(lastConfig, newConfig)){
                //配置发生变化
                target.reConfig(newConfig);
            }

            try {
                sleep(scanTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isChange(Properties lastConfig, Properties newConfig){
        if(lastConfig != null && newConfig != null){

        }
        return false;
    }

    public void close(){
        isStopped = true;
    }

}
