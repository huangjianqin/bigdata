package org.kin.kafka.multithread.configcenter.common.impl;

import org.ho.yaml.Yaml;
import org.kin.kafka.multithread.configcenter.common.StoreCodec;
import org.kin.kafka.multithread.configcenter.common.StoreCodecs;
import org.kin.kafka.multithread.configcenter.config.AppConfig;
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
        Map<String, String> result = new HashMap<>();
        YAMLUtils.transfer2Map((Map<String, Object>) Yaml.load(source), result, "");
        return result;
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
        Map<String, Object> yaml = new HashMap<>();
        YAMLUtils.transfer2Yaml(yaml, serialized);
        return Yaml.dump(yaml);
    }

    @Override
    public boolean vertify(String configStr) {
        return true;
    }
}
