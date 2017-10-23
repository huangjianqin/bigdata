package org.kin.kafka.multithread.configcenter;

import org.junit.Before;
import org.kin.kafka.multithread.configcenter.utils.JsonUtils;
import org.kin.kafka.multithread.configcenter.utils.PropertiesUtils;
import org.kin.kafka.multithread.configcenter.utils.YAMLUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/10/16.
 */
public class TestConfigBase {
    protected Properties properties;

    @Before
    public void init() throws IOException {
        //appConfig.properties
        properties = new Properties();
        String path = DiamondRestClient.class.getResource("/").getPath() + "appConfig.properties";
        properties.load(new FileInputStream(new File(path)));
    }

    public static String getPropertiesStr(Properties properties){
        StringBuilder sb = new StringBuilder();
        for(Object key: properties.keySet()){
            sb.append(key.toString() + "=" + properties.get(key).toString() + System.lineSeparator());
        }

        return sb.toString();
    }

    public static String getJSONConfig(Properties properties){
        return JsonUtils.map2Json(PropertiesUtils.properties2Map(properties));
    }

    public static String getYAMLConfig(Properties properties){
        return YAMLUtils.transfer2YamlStr(properties);
    }

}
