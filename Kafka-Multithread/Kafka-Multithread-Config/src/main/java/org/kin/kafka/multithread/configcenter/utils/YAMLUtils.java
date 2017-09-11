package org.kin.kafka.multithread.configcenter.utils;

import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class YAMLUtils {
    /**
     * 多层嵌套map
     * @param configPath
     * @return
     */
    public static Map<String, Object> loadYML(String configPath){
        try {
            return (Map<String, Object>) Yaml.load(new File(configPath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Properties loadYML2Properties(String configPath){
        Map<String, Object> yaml = loadYML(configPath);
        Properties properties = new Properties();
        transfer2Properties(yaml, properties, "");
        return properties;
    }

    /**
     * 将多层嵌套map转换成A.B.C的properties格式
     * @param yaml
     * @param properties
     * @param keyHead
     */
    public static void transfer2Properties(Map<String, Object> yaml, Properties properties, String keyHead){
        for(String key: yaml.keySet()){
            Object value = yaml.get(key);
            if(value instanceof HashMap){
                transfer2Properties((Map<String, Object>) value, properties, keyHead.equals("")? key : keyHead + "." + key);
            }
            else {
                if(value == null){
                    properties.put(keyHead.equals("")? key : keyHead + "." + key, "");
                }
                else{
                    properties.put(keyHead.equals("")? key : keyHead + "." + key, value);
                }
            }
        }
    }

    /**
     * 将多层嵌套map转换成A.B.C的properties的map格式
     * @param yaml
     * @param map
     */
    public static void transfer2Map(Map<String, Object> yaml, Map<String, String> map, String keyHead){
        for(String key: yaml.keySet()){
            Object value = yaml.get(key);
            if(value instanceof HashMap){
                transfer2Map((Map<String, Object>) value, map, keyHead.equals("")? key : keyHead + "." + key);
            }
            else {
                if(value == null){
                    map.put(keyHead.equals("")? key : keyHead + "." + key, "");
                }
                else{
                    map.put(keyHead.equals("")? key : keyHead + "." + key, value.toString());
                }
            }
        }
    }

    /**
     * 将A.B.C的properties的map格式转换多层嵌套map
     * @param yaml
     * @param properties
     */
    public static void transfer2Yaml(Map<String, Object> yaml, Map<String, String> config){
        for(Object key: config.keySet()){
            String keyStr = (String) key;
            if(keyStr.contains("\\.")){
                String[] split = keyStr.split("\\.", 2);
                Map<String, Object> nextLevel = yaml.containsKey(split[0])? (Map<String, Object>) yaml.get(split[0]) : new HashMap<>();
                if(!yaml.containsKey(split[0])){
                    yaml.put(split[0], nextLevel);
                }
                deepMap(nextLevel, split[1], config.get(key));
            }
        }
    }

    /**
     * 不断递归创建多层嵌套map
     * @param nowLevel
     * @param key 下面层数的key+.组成
     * @param value
     */
    private static void deepMap(Map<String, Object> nowLevel, String key, Object value){
        if(key.contains("\\.")){
            String[] split = key.split("\\.", 2);
            Map<String, Object> nextLevel = nowLevel.containsKey(split[0])? (Map<String, Object>) nowLevel.get(split[0]) : new HashMap<>();
            if(!nowLevel.containsKey(split[0])){
                nowLevel.put(split[0], nextLevel);
            }
            deepMap(nextLevel, split[1], value);
        }else{
            nowLevel.put(key, value);
        }
    }

    /**
     * 将A.B.C的properties格式转换多层嵌套map
     * @param yaml
     * @param properties
     */
    public static void transfer2Yaml(Map<String, Object> yaml, Properties config){
        for(Object key: config.keySet()){
            String keyStr = (String) key;
            if(keyStr.contains("\\.")){
                String[] split = keyStr.split("\\.", 2);
                Map<String, Object> nextLevel = yaml.containsKey(split[0])? (Map<String, Object>) yaml.get(split[0]) : new HashMap<>();
                if(!yaml.containsKey(split[0])){
                    yaml.put(split[0], nextLevel);
                }
                deepMap(nextLevel, split[1], config.get(key));
            }
        }
    }
}
