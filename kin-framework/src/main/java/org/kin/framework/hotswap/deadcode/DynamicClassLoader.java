//package org.kin.framework.hotswap.deadcode;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.InputStream;
//
///**
// * Created by huangjianqin on 2018/1/31.
// * 热更新专用ClassLoader
// * 凡是热更某个类，需用同一或子ClassLoader去重新加载引用了该类的类
// */
//public class DynamicClassLoader extends ClassLoader {
//    public DynamicClassLoader(ClassLoader parent) {
//        super(parent);
//    }
//
//    public DynamicClassLoader() {
//    }
//
//    public Class<?> loadClass(File file) throws Exception {
//        try (FileInputStream fis = new FileInputStream(file)) {
//            return loadClass(fis);
//        }
//    }
//
//    public Class<?> loadClass(InputStream is) throws Exception {
//        if (is.available() > 0) {
//            byte[] bytes = new byte[is.available()];
//            is.read(bytes);
//
//            return defineClass(null, bytes, 0, bytes.length);
//        }
//        return null;
//    }
//}
