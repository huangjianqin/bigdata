package org.kin.kafka.multithread.configcenter.codec.impl;

import com.alibaba.fastjson.JSONObject;
import org.kin.kafka.multithread.configcenter.codec.StoreCodec;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.utils.JsonUtils;

import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/11.
 * 序列化JSONObject
 */
public class JsonStoreCodec implements StoreCodec {
    @Override
    public Map<String, String> deSerialize(String source) {
        return JsonUtils.json2Map((JSONObject) JSONObject.parse(source));
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
        return JsonUtils.map2Json(serialized);
    }

    @Override
    public boolean vertify(String configStr) {
        return true;
    }
}
