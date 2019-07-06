//package org.kin.framework.hotswap.deadcode;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.nio.file.Path;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Created by huangjianqin on 2018/2/2.
// */
//public class CommonHotswapFactory extends AbstractHotswapFactory {
//    private static final Logger log = LoggerFactory.getLogger("hotSwap");
//    private List<ClassReloadableEntity> monitoredClassReferences = new ArrayList<>();
//
//    /**
//     * @param classReloadableEntity 注册该对象, 有类热更新, 尝试更新该实例的成员域
//     */
//    public void register(ClassReloadableEntity classReloadableEntity) {
//        monitoredClassReferences.add(classReloadableEntity);
//    }
//
//    @Override
//    public void reload(List<Path> changedPath) {
//        ClassLoader old = Thread.currentThread().getContextClassLoader();
//        DynamicClassLoader classLoader;
//        if (parent != null) {
//            classLoader = new DynamicClassLoader(parent);
//        } else {
//            classLoader = new DynamicClassLoader(old);
//        }
//
//        Thread.currentThread().setContextClassLoader(classLoader);
//
//        boolean isClassRedefineSuccess = true;
//        List<Class<?>> changedClasses = new ArrayList<>();
//        for (Path path : changedPath) {
//            boolean isSuccess = false;
//            Class<?> changedClass = null;
//            try {
//                changedClass = classLoader.loadClass(path.toFile());
//                changedClasses.add(changedClass);
//                isSuccess = true;
//            } catch (Exception e) {
//                isClassRedefineSuccess = false;
//                log.debug("hot swap class '" + changedClass.getName() + "' failure", e);
//            } finally {
//                if (isSuccess) {
//                    log.info("hot swap class '{}' success", changedClass.getName());
//                } else {
//                    log.info("hot swap class '{}' failure", changedClass.getName());
//                }
//            }
//        }
//
//        if (isClassRedefineSuccess) {
//            try {
//                for (Class<?> changedClass : changedClasses) {
//                    try {
//                        for (ClassReloadableEntity classReloadableEntity : monitoredClassReferences) {
//                            classReloadableEntity.reload(changedClass, classLoader);
//                        }
//                    } catch (Exception e) {
//                        log.error("replace new class '" + changedClass.getName() + "' reference failure", e);
//                    }
//
//                    log.info("replace new class '{}' reference success", changedClass.getName());
//                }
//            } finally {
//                //保存最新的classloader
//                parent = classLoader;
//            }
//        } else {
//            //遇到异常, 回退
//            Thread.currentThread().setContextClassLoader(old);
//        }
//    }
//}
