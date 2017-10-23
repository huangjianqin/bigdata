package org.kin.kafka.multithread.configcenter.manager.impl;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ConfigCenterConfig;
import org.kin.kafka.multithread.configcenter.manager.ConfigStoreManager;
import org.kin.kafka.multithread.configcenter.utils.PropertiesUtils;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class RedisConfigStoreManager  implements ConfigStoreManager{
    private static final Logger log = LoggerFactory.getLogger(ConfigStoreManager.class);

    private JedisPool pool;

    @Override
    public void setup(Properties config) {
        log.info("redis store manager setting up...");
        String host = (String) config.get(ConfigCenterConfig.CONFIG_STOREMANAGER_SERVER_HOST);
        String port = (String) config.get(ConfigCenterConfig.CONFIG_STOREMANAGER_SERVER_PORT);

        pool = new JedisPool(host, Integer.valueOf(port));
        log.info("redis store manager setted up");
    }

    @Override
    public void clearup() {
        log.info("redis store manager clearing up...");
        pool.close();
        log.info("redis store manager clear up finished");
    }

    @Override
    public boolean storeConfig(Properties appConfig) {
        try(Jedis client = pool.getResource()){
            String host = appConfig.getProperty(AppConfig.APPHOST);
            String appName = appConfig.getProperty(AppConfig.APPNAME);
            String key = String.format(KEY_FORMAT, host, appName);

            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            //先删除key
            pipeline.del(key);
            for(Map.Entry<Object, Object> entry: appConfig.entrySet()){
                pipeline.hset(key, entry.getKey().toString(), entry.getValue().toString());
            }
            Response<List<Object>> responses = pipeline.exec();
            pipeline.sync();

            boolean result = true;
            int i = 0;
            for(Map.Entry<Object, Object> entry: appConfig.entrySet()){
                if(!entry.getValue().equals(responses.get().get(i++))){
                    result = false;
                    break;
                }
            }

            return result;
        }
    }

    private Properties getOneAppConfig(String key){
        try(Jedis client = pool.getResource()){
            Properties config = new Properties();

            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            Response<Map<String, String>> keyvaluesResp = pipeline.hgetAll(key);
            pipeline.exec();
            pipeline.sync();

            for(Map.Entry<String, String> entry: keyvaluesResp.get().entrySet()){
                config.put(entry.getKey(), entry.getValue());
            }

            return config;
        }
    }

    @Override
    public Map<String, String> getAppConfigMap(ApplicationContextInfo appHost) {
        String key = String.format(KEY_FORMAT, appHost.getHost(), appHost.getAppName());
        return PropertiesUtils.properties2Map(getOneAppConfig(key));
    }

    @Override
    public List<Properties> getAllAppConfig(ApplicationContextInfo appHost) {
        List<Properties> configs = new ArrayList<>();
        try(Jedis jedis = pool.getResource()){
            String rootKey = String.format(KEY_FORMAT, appHost.getHost(), "*");
            Set<String> keys = jedis.keys(rootKey);

            for(String hkey: keys){
                configs.add(getOneAppConfig(hkey));
            }
        }
        return configs;
    }
}
