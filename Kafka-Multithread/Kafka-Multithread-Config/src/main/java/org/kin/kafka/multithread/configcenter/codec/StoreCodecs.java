package org.kin.kafka.multithread.configcenter.codec;

import org.kin.kafka.multithread.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class StoreCodecs {
    private static final Logger log = LoggerFactory.getLogger(StoreCodecs.class);

    private static Map<StoreType, StoreCodec> storeCodecMap = new HashMap<>();

    private static String storeCodecClasspath;
    static {
        log.info("loading store codecs...");
        storeCodecClasspath = StoreCodec.class.getName();
        storeCodecClasspath = storeCodecClasspath.substring(0, storeCodecClasspath.lastIndexOf("."));
        storeCodecClasspath += ".impl.%sStoreCodec";

        lazyLoadStoreCodecs(StoreType.values());
        log.info("all available store codecs in classpath load");
    }

    private static List<StoreCodec> lazyLoadStoreCodecs(StoreType... storeTypes){
        List<StoreCodec> storeCodecs = new ArrayList<>();
        for(StoreType storeType: storeTypes){
            //首字母大写
            String storeTypeDesc = storeType.getTypeDesc();
            char[] storeTypeDescArr = storeTypeDesc.toCharArray();
            storeTypeDescArr[0] -= 32;
            storeTypeDesc = new String(storeTypeDescArr);

            StoreCodec storeCodec = (StoreCodec) ClassUtils.instance(String.format(storeCodecClasspath, storeTypeDesc));
            storeCodecMap.put(storeType, storeCodec);

            storeCodecs.add(storeCodec);
            log.info(String.format("loaded '%s' store codec in classpath '%s'", storeType.getTypeDesc(), String.format(storeCodecClasspath, storeTypeDesc), storeTypeDesc));
        }

        return storeCodecs;
    }

    public static StoreCodec getCodecByName(String storeTypeDesc){
        StoreType storeType = StoreType.getByDesc(storeTypeDesc);
        if(storeType != null){
            return storeCodecMap.get(storeType);
        }
        else{
            //尝试全局Classpath搜索
            //...
            return null;
        }
    }

    public static StoreCodec getCodecByType(StoreType storeType){
        return storeCodecMap.get(storeType);
    }

    public enum StoreType {
        JOSN("json"), YAML("yaml"), PROPERTIES("properties");

        private String typeDesc;

        StoreType(String typeDesc) {
            this.typeDesc = typeDesc;
        }

        public static StoreType getByDesc(String typeDesc){
            for(StoreType type: values()){
                if(type.getTypeDesc().toLowerCase().equals(typeDesc.toLowerCase())){
                    return type;
                }
            }
            log.warn(String.format("StoreType enum doesn't has store type '%s'", typeDesc));
            return null;
        }

        public String getTypeDesc() {
            return typeDesc;
        }

        public void setTypeDesc(String typeDesc) {
            this.typeDesc = typeDesc;
        }
    }
}
