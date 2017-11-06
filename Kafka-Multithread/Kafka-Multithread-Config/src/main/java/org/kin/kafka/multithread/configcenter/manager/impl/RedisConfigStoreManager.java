package org.kin.kafka.multithread.configcenter.manager.impl;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ConfigCenterConfig;
import org.kin.kafka.multithread.configcenter.manager.AbstractConfigStoreManager;
import org.kin.kafka.multithread.configcenter.manager.ConfigStoreManager;
import org.kin.kafka.multithread.configcenter.utils.PropertiesUtils;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.utils.StringUtils;
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
public class RedisConfigStoreManager extends AbstractConfigStoreManager{
    //appHost:appName
    private static final String KEY_FORMAT = "%s:%s";
    //临时配置
    private static final String TMP_KEY_FORMAT = "%s:%s_tmp";
    //临时配置_是否被fetch的标识
    private static final String FLAG_KEY_FORMAT = "%s:%s_flag";
    private static final String FLAG_KEY_REGEX = "(.+)\\:(.+)_flag";
    private static final String FLAG_FETCHED = "1";
    private static final String FLAG_PENDING = "0";

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
    public boolean isCanStoreConfig(ApplicationContextInfo applicationContextInfo) {
        try(Jedis client = pool.getResource()){
            String flagKey = String.format(FLAG_KEY_FORMAT, applicationContextInfo.getHost(), applicationContextInfo.getAppName());
            return !client.exists(flagKey);
        }
    }

    @Override
    public boolean storeConfig(Properties appConfig) {
        try(Jedis client = pool.getResource()){
            String host = appConfig.getProperty(AppConfig.APPHOST);
            String appName = appConfig.getProperty(AppConfig.APPNAME);
            String key = String.format(TMP_KEY_FORMAT, host, appName);
            String flag = String.format(FLAG_KEY_FORMAT, host, appName);

            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            //先删除key
            pipeline.del(flag);
            pipeline.del(key);
            //写配置
            for(Map.Entry<Object, Object> entry: appConfig.entrySet()){
                pipeline.hset(key, entry.getKey().toString(), entry.getValue().toString());
            }
            //写flag
            Response<String> flagResponse = pipeline.set(flag, FLAG_PENDING);
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
            boolean flagResult = flagResponse.get().endsWith(FLAG_PENDING);

            return result && flagResult;
        }
    }

    @Override
    public boolean realStoreConfig(ApplicationContextInfo applicationContextInfo) {
        try(Jedis client = pool.getResource()){
            String host = applicationContextInfo.getHost();
            String appName = applicationContextInfo.getAppName();
            String key = String.format(TMP_KEY_FORMAT, host, appName);
            String realKey = String.format(KEY_FORMAT, host, appName);

            //重命名临时config的key,使其成为正式config
            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            Response<Boolean> isExists = pipeline.exists(key);
            Response<String> isRenameSucceed = pipeline.rename(key, realKey);
            Response<Long> isSetAppStatus = pipeline.hset(realKey, AppConfig.APPSTATUS, applicationContextInfo.getAppStatus().getStatusDesc());
            pipeline.exec();
            pipeline.sync();

            if (isExists.get()){
                if(isRenameSucceed.get().equals("OK") && isSetAppStatus.get() == 0){
                    if(delTmpConfig(applicationContextInfo)){
                        return true;
                    }
                    else{
                        log.error("'{}/{}''s delete tmp config fail", applicationContextInfo, appName);
                    }
                }
                else{
                    log.error("'{}/{}''s 'rename' operation or set right appStatus fail", applicationContextInfo, appName);
                }
            }
            else{
                log.error("'{}/{}''s tmp config doesn't exists", applicationContextInfo, appName);
            }

            return false;
        }
    }

    @Override
    public boolean delTmpConfig(ApplicationContextInfo applicationContextInfo) {
        try(Jedis client = pool.getResource()){
            String host = applicationContextInfo.getHost();
            String appName = applicationContextInfo.getAppName();
            String configKey = String.format(TMP_KEY_FORMAT, host, appName);
            String flag = String.format(FLAG_KEY_FORMAT, host, appName);

            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            Response<Long> delFlapResult = pipeline.del(flag);
            pipeline.del(configKey);
            pipeline.exec();
            pipeline.sync();

            return delFlapResult.get() == 1;
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

    private List<Properties> getAllAppConfig(String matchKey, String... excludes) {
        Set<String> excludeSet = new HashSet<>();
        if(excludes != null && excludes.length > 0){
            excludeSet = new HashSet<>(Arrays.asList(excludes));
        }

        List<Properties> configs = new ArrayList<>();
        try(Jedis jedis = pool.getResource()){
            Set<String> keys = jedis.keys(matchKey);

            for(String hkey: keys){
                if(!excludeSet.contains(hkey)){
                    configs.add(getOneAppConfig(hkey));
                }
            }
        }
        return configs;
    }

    @Override
    public Map<String, String> getAppConfigMap(ApplicationContextInfo applicationContextInfo) {
        String key = String.format(KEY_FORMAT, applicationContextInfo.getHost(), applicationContextInfo.getAppName());
        return PropertiesUtils.properties2Map(getOneAppConfig(key));
    }

    @Override
    public List<Properties> getAllAppConfig(ApplicationContextInfo appHost) {
        String rootKey = String.format(KEY_FORMAT, appHost.getHost(), "*");
        return getAllAppConfig(rootKey);
    }

    @Override
    public List<Properties> getAllTmpAppConfig(ApplicationContextInfo appHost) {
        String matchKey = String.format(TMP_KEY_FORMAT, appHost.getHost(), "*");
        Set<String> excludeSet = new HashSet<>();
        try(Jedis client = pool.getResource()){
            String matchFlag = String.format(FLAG_KEY_FORMAT, appHost.getHost(), "*");
            //获取cnofig flag
            List<String> matchedConfigFlag = new ArrayList<>(client.keys(matchFlag));
            List<Response<String>> responses = new ArrayList<>();

            Pipeline pipeline = client.pipelined();
            pipeline.multi();
            //获取flag值
            for(String flag: matchedConfigFlag){
                responses.add(pipeline.get(flag));
            }
            pipeline.exec();
            pipeline.sync();

            //判断哪些config是不可再次fetcher
            for(int i = 0; i < responses.size(); i++){
                //只获取还没fetch过的app config
                if(responses.get(i).get().equals(FLAG_PENDING)){
                    //设置fetch成功
                    client.set(matchedConfigFlag.get(i), FLAG_FETCHED);
                }
                else{
                    //fetch过就不再fetch
                    String[] hostAndAppName = StringUtils.getHostAndAppName(FLAG_KEY_REGEX, matchedConfigFlag.get(i));
                    excludeSet.add(String.format(TMP_KEY_FORMAT, hostAndAppName[0], hostAndAppName[1]));
                }
            }

            //如果没有任何对应的config flag, 则直接返回有个空lsit
            if(matchedConfigFlag.size() > 0){
                if(excludeSet.size() > 0){
                    return getAllAppConfig(matchKey, excludeSet.toArray(new String[1]));
                }
                else{
                    return getAllAppConfig(matchKey);
                }
            }
        }
        return new ArrayList<>();
    }
}
