package org.kin.framework.hotswap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by huangjianqin on 2018/10/31.
 * 单例模式 每次热更后, 都会检查版本号, 执行开发者自定义逻辑
 */
public class HotFix extends ClassReloadable {
    private static HotFix HOT_FIX = new HotFix();
    private static final Logger log = LoggerFactory.getLogger("hot-fix-class");

    //记录上次更新的版本号, 此处仅仅缓存, 以后可选择保存在数据库或文件
    private int oldVersion;
    private int version;

    private HotFix() {
    }

    public static HotFix instance(){
        return HOT_FIX;
    }

    @Override
    void reload(Class<?> changedClass) {
        super.reload(changedClass);
    }

    public void fix(){
        //延迟10s执行
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                fix0();
            }
        }, 10 * 1000);
    }

    public void fix0(){
        if(oldVersion < version){
            log.info("hot fix start: {}", version);
            try{
                /**
                 * 此处写逻辑
                 */
            }finally {
                oldVersion = version;
                version = 0;
                log.info("hot fix finish: {}", oldVersion);
            }
        }
    }
}
