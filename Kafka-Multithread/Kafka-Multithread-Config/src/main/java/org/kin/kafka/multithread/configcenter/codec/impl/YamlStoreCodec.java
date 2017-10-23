package org.kin.kafka.multithread.configcenter.codec.impl;

import org.ho.yaml.Yaml;
import org.kin.kafka.multithread.configcenter.codec.StoreCodec;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.utils.YAMLUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/11.
 * 序列化成多层嵌套Map
 */
public class YamlStoreCodec implements StoreCodec {
    @Override
    public Map<String, String> deSerialize(String source) {
        return YAMLUtils.transfer2Map((Map<String, Object>) Yaml.load(source));
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
        return YAMLUtils.transfer2YamlStr(YAMLUtils.transfer2Yaml(serialized));
    }

    @Override
    public boolean vertify(String configStr) {
        return true;
    }
}
