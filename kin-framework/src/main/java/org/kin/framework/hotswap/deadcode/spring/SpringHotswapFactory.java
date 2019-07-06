//package org.kin.framework.hotswap.deadcode.spring;
//
//import com.google.common.collect.ArrayListMultimap;
//import com.google.common.collect.Multimap;
//import org.kin.framework.hotswap.deadcode.AbstractHotswapFactory;
//import org.kin.framework.hotswap.deadcode.DynamicClassLoader;
//import org.kin.framework.utils.ClassUtils;
//import org.kin.framework.utils.ExceptionUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor;
//import org.springframework.beans.factory.annotation.InjectionMetadata;
//import org.springframework.beans.factory.config.BeanDefinition;
//import org.springframework.beans.factory.config.BeanPostProcessor;
//import org.springframework.beans.factory.support.DefaultListableBeanFactory;
//import org.springframework.context.ApplicationListener;
//import org.springframework.context.annotation.ScannedGenericBeanDefinition;
//import org.springframework.context.event.ContextRefreshedEvent;
//
//import java.io.File;
//import java.lang.reflect.Field;
//import java.net.URL;
//import java.nio.file.Path;
//import java.util.*;
//
///**
// * Created by huangjianqin on 2018/1/31.
// * 目前仅仅支持AutoWired注解的bean
// */
////@Component
//public class SpringHotswapFactory extends AbstractHotswapFactory implements ApplicationListener<ContextRefreshedEvent> {
//    private static final Logger log = LoggerFactory.getLogger("hotSwap");
//    private DefaultListableBeanFactory beanFactory;
//    //缓存所有BeanDefinition
//    //hash(class name) -> set(BeanDefinitionDetail)
//    private Multimap<Integer, BeanDefinitionDetail> beanDefinitionDetailsMap = ArrayListMultimap.create();
//    //bean name -> BeanDefinitionDetail
//    private Map<String, BeanDefinitionDetail> name2DefinitionDetailsMap = new HashMap<>();
//
//    @Override
//    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
//        //最终DefaultListableBeanFactory注册bean
//        beanFactory = (DefaultListableBeanFactory) contextRefreshedEvent.getApplicationContext();
//
//        for (String beanName : beanFactory.getBeanDefinitionNames()) {
//            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
//            try {
//                Class<?> beanClass = Class.forName(beanDefinition.getBeanClassName());
//                URL res = beanClass.getResource("");
//                String packageDir = res.getPath();
//                String classFileName = beanClass.getSimpleName() + ClassUtils.CLASS_SUFFIX;
//                File file = new File(packageDir + classFileName);
//
//                BeanDefinitionDetail beanDefinitionDetail = new BeanDefinitionDetail(beanName, beanDefinition, file);
//                beanDefinitionDetailsMap.put(beanClass.getName().hashCode(), beanDefinitionDetail);
//                name2DefinitionDetailsMap.put(beanName, beanDefinitionDetail);
//            } catch (ClassNotFoundException e) {
//                ExceptionUtils.log(e);
//            }
//        }
//    }
//
//
//    @Override
//    public void reload(List<Path> changedPath) {
//        List<Class<?>> changedClasses = new ArrayList<>();
//        //加载最新的class
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
//        for (Path path : changedPath) {
//            boolean isSuccess = false;
//            Class<?> changedClass = null;
//            try {
//                changedClass = classLoader.loadClass(path.toFile());
//                changedClasses.add(changedClass);
//                isSuccess = true;
//                classLoader = reload(changedClass, classLoader);
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
//                injectNewBeans(changedClasses);
//            } finally {
//                parent = classLoader;
//            }
//        } else {
//            //遇到异常, 回退
//            Thread.currentThread().setContextClassLoader(old);
//        }
//    }
//
//    /**
//     * 热加载class
//     *
//     * @param changedClass 新Class
//     * @param parent       加载了changedClass的classloader
//     */
//    private DynamicClassLoader reload(Class<?> changedClass, DynamicClassLoader parent) {
//        Collection<BeanDefinitionDetail> beanDefinitionDetails = beanDefinitionDetailsMap.get(changedClass.getName().hashCode());
//        if (beanDefinitionDetails.isEmpty()) {
//            return parent;
//        }
//
//        for (BeanDefinitionDetail beanDefinitionDetail : beanDefinitionDetails) {
//            //@Autowire的注入细节
//            ScannedGenericBeanDefinition beanDefinition = (ScannedGenericBeanDefinition) beanDefinitionDetail.getBeanDefinition();
//            beanDefinition.setBeanClass(changedClass);
//            //删除beanFactory缓存的bean
//            beanFactory.destroySingleton(beanDefinitionDetail.getBeanName());
//            //重新构建bean
//            Object bean = beanFactory.getBean(beanDefinitionDetail.getBeanName());
//            parent = reloadDependentBeans(beanDefinitionDetail, bean, parent);
//        }
//
//        return parent;
//    }
//
//    /**
//     * 重新加载引用(依赖)@param newBean实例的bean
//     */
//    private DynamicClassLoader reloadDependentBeans(BeanDefinitionDetail beanDefinitionDetail, Object newBean, DynamicClassLoader parent) {
//        //获取依赖@param newBean的bean
//        String[] dependentbeanNames = beanFactory.getDependentBeans(beanDefinitionDetail.getBeanName());
//        if (dependentbeanNames.length <= 0) {
//            return parent;
//        }
//        DynamicClassLoader child = new DynamicClassLoader(parent);
//        Thread.currentThread().setContextClassLoader(child);
//
//        for (String dependentBeanName : dependentbeanNames) {
//            BeanDefinitionDetail dependentBeanDefinitionDetail = name2DefinitionDetailsMap.get(dependentBeanName);
//            //重新加载
//            Class<?> dependentBeanClass = null;
//            try {
//                dependentBeanClass = child.loadClass(dependentBeanDefinitionDetail.getFile());
//            } catch (Exception e) {
//                ExceptionUtils.log(e);
//            }
//
//            //获取该bean所有AutoWired注入
//            Collection<InjectionMetadata.InjectedElement> injectedElements = getAffectedInjectElements(dependentBeanName);
//            //遍历所有Field
//            for (Field field : ClassUtils.getAllFields(dependentBeanClass)) {
//                Class<?> type = field.getType();
//
//                if (type.isInstance(newBean)) {
//                    //成员变量类型与@param newBean类型符合
//                    for (InjectionMetadata.InjectedElement element : injectedElements) {
//                        Field injectedField = (Field) element.getMember();
//                        if (injectedField.getType().getName().equals(type.getName())
//                                && field.getName().equals(injectedField.getName())) {
//                            //替换成新的Field
//                            ClassUtils.setFieldValue(element, "member", field);
//                            break;
//                        }
//                    }
//                }
//            }
//
//            //clear 依赖bean缓存,并重新构建且广播类更新
//            child = reload(dependentBeanClass, child);
//        }
//
//        return child;
//    }
//
//    /**
//     * 获取注入的InjectedElement(仅仅是@Autowired注入)
//     */
//    private Collection<InjectionMetadata.InjectedElement> getAffectedInjectElements(String beanName) {
//        BeanPostProcessor beanPostProcessor = getAutowiredAnnotationBeanPostProcessor();
//        if (beanPostProcessor != null) {
//            Map<String, InjectionMetadata> injectionMetadataMap = ClassUtils.getFieldValue(beanPostProcessor, "injectionMetadataCache");
//            if (injectionMetadataMap != null && injectionMetadataMap.size() > 0) {
//                InjectionMetadata injectionMetadata = injectionMetadataMap.get(beanName);
//                if (injectionMetadata != null) {
//                    Collection<InjectionMetadata.InjectedElement> elements = ClassUtils.getFieldValue(injectionMetadata, "checkedElements");
//                    if (elements == null) {
//                        elements = ClassUtils.getFieldValue(injectionMetadata, "injectedElements");
//                    }
//
//                    if (elements != null) {
//                        return elements;
//                    }
//                }
//            }
//        }
//
//        return Collections.emptySet();
//    }
//
//    /**
//     * 获取AutowiredAnnotationBeanPostProcessor
//     */
//    private BeanPostProcessor getAutowiredAnnotationBeanPostProcessor() {
//        for (BeanPostProcessor beanPostProcessor : beanFactory.getBeanPostProcessors()) {
//            if (beanPostProcessor instanceof AutowiredAnnotationBeanPostProcessor) {
//                return beanPostProcessor;
//            }
//        }
//
//        return null;
//    }
//
//    /**
//     * 重新注入newBean引用
//     */
//    private void injectNewBeans(List<Class<?>> changedClasses) {
//        for (Class<?> changedClass : changedClasses) {
//            try {
//                Collection<BeanDefinitionDetail> beanDefinitionDetails = beanDefinitionDetailsMap.get(changedClass.getName().hashCode());
//
//                if (!beanDefinitionDetails.isEmpty()) {
//                    for (BeanDefinitionDetail beanDefinitionDetail : beanDefinitionDetails) {
//                        Object newBean = beanFactory.getBean(beanDefinitionDetail.getBeanName());
//                        injectDependentBean(beanDefinitionDetail, newBean);
//                    }
//                    log.info("reinject new class '{}' reference success", changedClass.getName());
//                }
//            } catch (Exception e) {
//                log.error("reinject new class '" + changedClass.getName() + "' reference failure", e);
//            }
//
//        }
//
//    }
//
//    /**
//     * 为依赖@param newBean的bean重新注入newBean
//     */
//    private void injectDependentBean(BeanDefinitionDetail beanDefinitionDetail, Object newBean) {
//        //获取依赖@param newBean的bean
//        String[] dependentbeanNames = beanFactory.getDependentBeans(beanDefinitionDetail.getBeanName());
//        if (dependentbeanNames.length <= 0) {
//            return;
//        }
//
//        for (String dependentBeanName : dependentbeanNames) {
//            Object dependentBean = beanFactory.getBean(dependentBeanName);
//
//            BeanDefinitionDetail dependentBeanDefinitionDetail = name2DefinitionDetailsMap.get(dependentBeanName);
//            //获取被依赖bean的class
//            Class<?> dependentBeanClass = ((ScannedGenericBeanDefinition) dependentBeanDefinitionDetail.getBeanDefinition()).getBeanClass();
//
//            //获取该bean所有AutoWired注入
//            Collection<InjectionMetadata.InjectedElement> injectedElements = getAffectedInjectElements(dependentBeanName);
//            //遍历所有Field
//            for (Field field : ClassUtils.getAllFields(dependentBeanClass)) {
//                Class<?> type = field.getType();
//
//                if (type.isInstance(newBean)) {
//                    //成员变量类型与@param newBean类型符合
//                    for (InjectionMetadata.InjectedElement element : injectedElements) {
//                        Field injectedField = (Field) element.getMember();
//                        if (injectedField.getType().getName().equals(type.getName())
//                                && field.getName().equals(injectedField.getName())) {
//                            //替换成新的Field
//                            ClassUtils.setFieldValue(dependentBean, field, newBean);
//                            break;
//                        }
//                    }
//                }
//            }
//        }
//    }
//}
