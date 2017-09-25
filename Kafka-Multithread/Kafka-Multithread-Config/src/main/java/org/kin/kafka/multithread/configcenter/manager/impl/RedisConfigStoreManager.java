package org.kin.kafka.multithread.configcenter.manager.impl;

import org.kin.kafka.multithread.configcenter.common.StoreCodec;
import org.kin.kafka.multithread.configcenter.common.StoreCodecs;
import org.kin.kafka.multithread.configcenter.config.AppConfig;
import org.kin.kafka.multithread.configcenter.config.ConfigCenterConfig;
import org.kin.kafka.multithread.configcenter.manager.ConfigStoreManager;
import org.kin.kafka.multithread.protocol.app.ApplicationConfig;
import org.kin.kafka.multithread.protocol.app.ApplicationHost;
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

            client.multi();
            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            for(Map.Entry<Object, Object> entry:appConfig.entrySet()){
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

    @Override
    public Map<String, String> getAppConfigMap(ApplicationHost appHost) {
        try(Jedis client = pool.getResource()){
            Map<String, String> config = new HashMap<>();
            String key = String.format(KEY_FORMAT, appHost.getHost(), appHost.getAppName());
            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            for(String hKey: client.hkeys(key)){
               pipeline.hget(key, hKey).get();
            }
            Response<List<Object>> responses = pipeline.exec();
            pipeline.sync();

            boolean result = true;
            int i = 0;
            for(String hKey: client.hkeys(key)){
                Object o = responses.get().get(i ++);
                if(o != null){
                    config.put(hKey, o.toString());
                }
                else{
                    result = false;
                    break;
                }
            }

            if(result){
                return config;
            }
        }
        return null;
    }

    @Override
    public List<Properties> getAllAppConfig(ApplicationHost appHost) {
        List<Properties> configs = new ArrayList<>();
        try(Jedis jedis = pool.getResource()){
            String rootKey = String.format(KEY_FORMAT, appHost.getHost(), "");
            Set<String> keys = jedis.keys(rootKey);
            for(String configKey: keys){
                Set<String> childKeys = jedis.hkeys(configKey);
                Pipeline pipeline = jedis.pipelined();
                pipeline.multi();
                for(String childKey: childKeys){
                    pipeline.hget(childKey, childKey);
                }
                List<Object> result = pipeline.exec().get();
                pipeline.sync();

                String[] childKeysArr = childKeys.toArray(new String[1]);

                Properties config = new Properties();
                for(int i = 0; i < childKeysArr.length; i++){
                    config.put(childKeysArr[i], result.get(i));
                }

                configs.add(config);
            }
        }
        return configs;
    }
}
