package org.kin.kafka.multithread.config;

import java.util.Map;
import java.util.Properties;

/**
 * Created by hjq on 2017/6/19.
 * 对Properties的简单封装成
 * 添加函数式风格及不可变对象实现
 */
public class PropertiesWrapper {
    private Properties properties;
    public PropertiesWrapper() {
        this.properties = new Properties();
    }

    public PropertiesWrapper(Properties properties) {
        this.properties = properties;
    }

    public PropertiesWrapper(Map<String, String> properties) {
        this.properties = new Properties();
        for(Map.Entry<String, String> entry: properties.entrySet()){
            this.properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    public PropertiesWrapper(String key, String value) {
        this.properties = new Properties();
        this.properties.setProperty(key, value);
    }

    public static PropertiesWrapper create(){
        return new PropertiesWrapper();
    }

    public static PropertiesWrapper create(Properties properties){
        return new PropertiesWrapper(properties);
    }

    public static PropertiesWrapper create(Map<String, String> properties){
        return new PropertiesWrapper(properties);
    }

    public static PropertiesWrapper create(String key, String value){
        return new PropertiesWrapper(key, value);
    }


    public PropertiesWrapper set(Properties properties){
        return new PropertiesWrapper(newPropertys(properties));
    }

    public PropertiesWrapper set(Map<String, String> properties){
        return new PropertiesWrapper(newPropertys(properties));
    }

    public PropertiesWrapper set(String key, String value){
        return new PropertiesWrapper(newPropertys(key, value));
    }

    public Properties properties(){
        return this.properties;
    }

    private void copyOrigin(Properties target){
        for(String key: this.properties.stringPropertyNames()){
            target.setProperty(key, properties.getProperty(key));
        }
    }

    private Properties newPropertys(Properties properties){
        Properties result = new Properties();
        copyOrigin(result);
        for(String key: properties.stringPropertyNames()){
            result.setProperty(key, properties.getProperty(key));
        }
        return result;
    }
    private Properties newPropertys(Map<String, String> properties){
        Properties result = new Properties();
        copyOrigin(result);
        for(Map.Entry<String, String> entry: properties.entrySet()){
            result.setProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }
    private Properties newPropertys(String key, String value){
        Properties result = new Properties();
        copyOrigin(result);
        result.setProperty(key, value);
        return result;
    }
}
