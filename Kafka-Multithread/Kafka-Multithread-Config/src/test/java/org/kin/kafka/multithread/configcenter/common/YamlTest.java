package org.kin.kafka.multithread.configcenter.common;

import org.kin.kafka.multithread.configcenter.utils.YAMLUtils;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class YamlTest {
    public static void main(String[] args){
        Properties properties = YAMLUtils.loadYML2Properties("E:\\javawebapps\\BigData\\Kafka-Multithread\\Kafka-Multithread-ConfigCenterConfig\\src\\test\\resources\\configcenter.yml");
        for(Object key: properties.keySet()){
            System.out.println(key + ">>" + properties.get(key));
        }
    }
}
