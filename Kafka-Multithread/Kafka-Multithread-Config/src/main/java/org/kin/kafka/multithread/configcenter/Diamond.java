package org.kin.kafka.multithread.configcenter;

import org.apache.log4j.Level;
import org.kin.framework.log.Log4jLoggerBinder;
import org.kin.kafka.multithread.configcenter.codec.StoreCodec;
import org.kin.kafka.multithread.configcenter.codec.StoreCodecs;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.distributed.AppStatus;
import org.kin.kafka.multithread.configcenter.manager.ConfigStoreManager;
import org.kin.kafka.multithread.configcenter.utils.ConfigCenterConfigUtils;
import org.kin.kafka.multithread.configcenter.utils.PropertiesUtils;
import org.kin.kafka.multithread.configcenter.utils.YAMLUtils;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatResponse;
import org.kin.kafka.multithread.domain.ConfigFetcherHeartbeatRequest;
import org.kin.kafka.multithread.protocol.app.ApplicationContextInfo;
import org.kin.kafka.multithread.protocol.configcenter.AdminProtocol;
import org.kin.kafka.multithread.protocol.configcenter.DiamondMasterProtocol;
import org.kin.kafka.multithread.rpc.factory.RPCFactories;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 * 配置中心实现类
 *
 * 可以利用dubbo(目前底层的RPC)实现配置中心的服务化,可以解决配置中心挂掉无法提供服务以及通过负载均衡提高配置中心处理能力
 * 考虑实际生产环境,可能部署的kafka多线程消费应用数并不多,其实配置中心的压力不大,可以考虑不使用服务化
 *
 * 可以完美切换底层RPC(自定义实现RPCFactory并注入到RPCFactories)和配置存储(自定义实现ConfigStoreManager并在配置文件中修改class)
 */
@Path("kafkamultithread")
public class Diamond implements DiamondMasterProtocol, AdminProtocol{
    static {log();}
    private static final Logger log = LoggerFactory.getLogger("Diamond");

    private ConfigStoreManager configStoreManager;
    private Properties config = new Properties();

    public Diamond() {
        this(DefaultConfigCenterConfig.DEFALUT_CONFIGPATH);
    }

    public Diamond(String configPath) {
        config = YAMLUtils.loadYML2Properties(configPath);
        ConfigCenterConfigUtils.oneNecessaryCheckAndFill(config);
        log.info("diamond loaded config " + System.lineSeparator() + ConfigCenterConfigUtils.toString(config));
    }

    public void init(){
        log.info("diamond initing...");
        String storeManagerClass = (String) config.getOrDefault(ConfigCenterConfig.CONFIG_STOREMANAGER_CLASS, DefaultConfigCenterConfig.DEFAULT_CONFIG_STOREMANAGER_CLASS);
        configStoreManager = (ConfigStoreManager) ClassUtils.instance(storeManagerClass);
        configStoreManager.setup(config);

        //加载所有已写进StoreType的codec
        try {
            Class.forName(StoreCodecs.class.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        log.info("diamond inited");
    }

    /**
     * 如果没有适合的logger使用api创建默认logger
     */
    private static void log(){
        String logger = "Diamond";
        if(!Log4jLoggerBinder.exist(logger)){
            String appender = "diamond";
            Log4jLoggerBinder.create()
                    .setLogger(Level.INFO, logger, appender)
                    .setDailyRollingFileAppender(appender)
                    .setFile(appender, "/tmp/kafka-multithread/config/diamond.log")
                    .setDatePattern(appender)
                    .setAppend(appender, true)
                    .setThreshold(appender, Level.INFO)
                    .setPatternLayout(appender)
                    .setConversionPattern(appender)
                    .bind();
        }
    }

    public void start(){
        log.info("diamond starting...");
        init();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));

        RPCFactories.serviceWithoutRegistry(
                DiamondMasterProtocol.class,
                this,
                Integer.valueOf(config.getProperty(ConfigCenterConfig.DIAMONDMASTERPROTOCOL_PORT))
        );
        log.info(String.format("Diamond RPC interface bind '%s' port", config.getProperty(ConfigCenterConfig.DIAMONDMASTERPROTOCOL_PORT)));
        RPCFactories.restServiceWithoutRegistry(AdminProtocol.class,
                this,
                Integer.valueOf(config.getProperty(ConfigCenterConfig.ADMINPROTOCOL_PORT))
        );
        log.info(String.format("Admin RPC interface bind '%s' port", config.getProperty(ConfigCenterConfig.ADMINPROTOCOL_PORT)));


        log.info("diamond started");

        while (true){
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config/post/{appName}/{type}")
    public Map<String, Object> storeConfig(
            @PathParam("appName") String appName,
            @FormParam("host") String host,
            @PathParam("type") String type,
            @FormParam("config") String config
    ) {
        Map<String, Object> result = new HashMap<>();

        log.info("store app config from app '" + appName + "' on host '" + host + "'" + System.lineSeparator() + config);

        if(!configStoreManager.isCanStoreConfig(new ApplicationContextInfo(appName, host))){
            result.put("result", -1);
            result.put("info", String.format("{}'s last config update hasn't be finished", appName));
            return result;
        }

        StoreCodec storeCodec = StoreCodecs.getCodecByName(type);
        //保证host与appName一致性
        Properties configProperties = PropertiesUtils.map2Properties(storeCodec.deSerialize(config));
        configProperties.put(AppConfig.APPHOST, host);
        configProperties.put(AppConfig.APPNAME, appName);

        //如果是更新配置,则只能更新指定配置,其余配置只能固定
        if(AppStatus.getByStatusDesc(configProperties.getProperty(AppConfig.APPSTATUS)).equals(AppStatus.UPDATE)){
            Properties origin = PropertiesUtils.map2Properties(configStoreManager.getAppConfigMap(new ApplicationContextInfo(appName, host)));
            for(Object key: origin.keySet()){
                if(!AppConfig.CAN_RECONFIG_APPCONFIGS.contains(key)){
                    if(origin.containsKey(key)){
                        if(!origin.get(key).equals(configProperties.get(key))){
                            result.put("result", -1);
                            result.put("info", String.format("config '%s' can't not be change when app reconfiging", key));
                            return result;
                        }
                    }
                    else{
                        result.put("result", -1);
                        result.put("info", String.format("config '%s' can't not be change when app reconfiging", key));
                        return result;
                    }
                }
            }
        }

        //缓存在redis上
        configStoreManager.storeConfig(configProperties);

        result.put("result", 1);
        return result;
    }

    /**
     * 仅仅获取持久化的配置,也就是应用的真实配置
     * @param appName
     * @param host
     * @param type
     * @return
     */
    @Override
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config/get/{appName}/{type}")
    public Map<String, Object> getAppConfigStr(
            @PathParam("appName") String appName,
            @QueryParam("host") String host,
            @PathParam("type") String type
    ) {
        log.info("get app config string from app '" + appName + "' on host '" + host + "' tansfer to '" + type + "' string from rest call");
        ApplicationContextInfo applicationContextInfo = new ApplicationContextInfo(appName, host);

        Map<String, String> config = configStoreManager.getAppConfigMap(applicationContextInfo);
        if(config != null){
            StoreCodec storeCodec = StoreCodecs.getCodecByName(type);
            String configStr = storeCodec.serialize(config);
            Map<String, Object> result = new HashMap<>();
            result.put("result", (configStr != null && !configStr.equals(""))? 1 : 0);
            result.put("type", type);
            result.put("config", configStr);
            return result;
        }

        return Collections.singletonMap("result", 0);
    }

    @Override
    public ConfigFetcherHeartbeatResponse heartbeat(ConfigFetcherHeartbeatRequest heartbeat) {
        for(ApplicationContextInfo succeedApplicationContextInfo: heartbeat.getSucceedAppNames()){
            //持久化
            configStoreManager.realStoreConfig(succeedApplicationContextInfo);
        }

        for(ApplicationContextInfo failApplicationContextInfo: heartbeat.getFailAppNames()){
            configStoreManager.delTmpConfig(failApplicationContextInfo);
            //通知Admin
        }

        ApplicationContextInfo appHost = heartbeat.getAppHost();

        log.debug("get app config from on host '{}'", appHost.getHost());
        ConfigFetcherHeartbeatResponse result = new ConfigFetcherHeartbeatResponse(configStoreManager.getAllTmpAppConfig(appHost), System.currentTimeMillis());

        return result;
    }

    public void close(){
        log.info("diamond closing...");
        configStoreManager.clearup();
        log.info("diamond closed");
    }

    public Properties getConfig() {
        return config;
    }

}
