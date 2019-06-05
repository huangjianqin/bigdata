package org.kin.framework;

/**
 * Created by huangjianqin on 2019/2/28.
 * <p>
 * close服务并释放资源
 */
public interface Closeable {
    void close();

    default void monitorJVMClose(){
        JvmCloseCleaner.DEFAULT().add(this);
    }
}
