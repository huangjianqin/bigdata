package org.kin.kafka.multithread.configcenter.common.impl;

import org.kin.kafka.multithread.configcenter.common.StoreCodec;
import org.kin.kafka.multithread.configcenter.common.StoreCodecs;
import org.kin.kafka.multithread.configcenter.config.AppConfig;
import org.kin.kafka.multithread.configcenter.utils.PropertiesUtils;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class PropertiesStoreCodec implements StoreCodec {
    @Override
    public Map<String, String> deSerialize(String source) {
        Properties properties = new Properties();
        try {
            try(StringReader reader = new StringReader(source)){
                properties.load(reader);
                return PropertiesUtils.properties2Map(properties);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Map<String, String> merge(String sourceConfig, String appName, String host) {
        Map<String, String> map = deSerialize(sourceConfig);
        map.put(AppConfig.APPNAME, appName);
        map.put(AppConfig.APPHOST, host);
        return map;
    }

    @Override
    public String serialize(Map<String, String> serialized) {
        Properties properties = PropertiesUtils.map2Properties(serialized);
        try {
            try(StringWriter writer = new StringWriter()){
                properties.store(writer, "");
                return writer.toString();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    @Override
    public boolean vertify(String configStr) {
        return true;
    }
}
