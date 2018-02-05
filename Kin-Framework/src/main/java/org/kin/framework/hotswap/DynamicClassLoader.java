package org.kin.framework.hotswap;

import org.kin.framework.utils.ExceptionUtils;
import java.io.*;

/**
 * Created by huangjianqin on 2018/1/31.
 * 热更新专用ClassLoader
 * 凡是热更某个类，需用同一或子ClassLoader去重新加载引用了该类的类
 */
public class DynamicClassLoader extends ClassLoader{
    public DynamicClassLoader(ClassLoader parent) {
        super(parent);
    }

    public DynamicClassLoader() {
    }

    public Class<?> loadClass(File file){
        try(FileInputStream fis = new FileInputStream(file)){
            return loadClass(fis);
        } catch (FileNotFoundException e) {
            ExceptionUtils.log(e);
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }

    public Class<?> loadClass(InputStream is){
        try {
            if(is.available() > 0) {
                byte[] bytes = new byte[is.available()];
                is.read(bytes);

                return defineClass(null, bytes, 0, bytes.length);
            }
        } catch (FileNotFoundException e) {
            ExceptionUtils.log(e);
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }
}
