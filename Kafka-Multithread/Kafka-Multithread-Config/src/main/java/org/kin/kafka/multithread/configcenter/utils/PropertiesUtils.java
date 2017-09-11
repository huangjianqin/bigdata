package org.kin.kafka.multithread.configcenter.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class PropertiesUtils {
    public static Map<String, String> properties2Map(Properties properties){
        Map<String, String> result = new HashMap<>();
        for(Object key: properties.keySet()){
            result.put(key.toString(), properties.get(key).toString());
        }
        return result;
    }

    public static Properties map2Properties(Map<String, String> map){
        Properties properties = new Properties();
        for(String key: map.values()){
            properties.put(key, map.get(key));
        }
        return properties;
    }
}
