package org.kin.framework.asyncdb;

import org.kin.framework.Closeable;
import org.kin.framework.asyncdb.impl.DefaultAsyncDBStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by huangjianqin on 2019/3/31.
 *
 * 一个Entity(以hashcode分区)一条线程执行DB操作
 */
@Component
public class AsyncDBService implements ApplicationContextAware, Closeable{
    private static final Logger log = LoggerFactory.getLogger("asyncDB");
    private ApplicationContext springContext;
    private static AsyncDBService instance;

    private final Map<Class, DBSynchronzier> class2Persistent = new ConcurrentHashMap<>();
    private AsyncDBExecutor asyncDBExecutor;

    //---------------------------------------------------------------------------------------------------
    public static AsyncDBService getInstance() {
        return instance;
    }

    @Autowired
    public void setInstance(AsyncDBService instance) {
        AsyncDBService.instance = instance;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        springContext = applicationContext;
    }
    //---------------------------------------------------------------------------------------------------
    public AsyncDBService() {
        monitorJVMClose();
    }

    public void init(int num, AsyncDBStrategy asyncDBStrategy){
        asyncDBExecutor = new AsyncDBExecutor();
        asyncDBExecutor.init(num, asyncDBStrategy);
    }

    @PostConstruct
    public void springInit(){
        asyncDBExecutor = new AsyncDBExecutor();
        asyncDBExecutor.init(10, new DefaultAsyncDBStrategy());
    }

    /**
     * 手动注册持久化实现类
     */
    public void register(Class<?> claxx, DBSynchronzier DBSynchronzier){
        Type interfaceType = null;
        for(Type type: DBSynchronzier.getClass().getGenericInterfaces()){
            if(type instanceof ParameterizedType && ((ParameterizedType)type).getRawType().equals(DBSynchronzier.class)){
                interfaceType = type;
                break;
            }
            if(type instanceof Class && type.equals(DBSynchronzier.class)){
                interfaceType = type;
                break;
            }
        }

        if(interfaceType != null){
            Type entityType = ((ParameterizedType) interfaceType).getActualTypeArguments()[0];
            Class<?> entityClass;
            if(entityType instanceof ParameterizedType){
                entityClass = (Class<?>) ((ParameterizedType) entityType).getRawType();
            }
            else{
                entityClass = (Class<?>) entityType;
            }

            if(entityClass.isAssignableFrom(claxx)){
                //校验通过
                class2Persistent.put(claxx, DBSynchronzier);
            }
        }
    }

    /**
     * 自动从Spring容器获取持久化实现类
     */
    private DBSynchronzier getAsyncPersistent(AsyncDBEntity asyncDBEntity){
        Class claxx = asyncDBEntity.getClass();
        if(!class2Persistent.containsKey(claxx)){
            PersistentClass persistentAnnotation = (PersistentClass) claxx.getAnnotation(PersistentClass.class);
            if(persistentAnnotation != null){
                Class<? extends DBSynchronzier> persistentClass = persistentAnnotation.type();
                if (persistentClass != null) {
                    DBSynchronzier DBSynchronzier = springContext.getBean(persistentClass);
                    if(DBSynchronzier != null){
                        class2Persistent.put(claxx, DBSynchronzier);
                        return DBSynchronzier;
                    }
                    else {
                        throw new AsyncDBException("找不到类'" + claxx.getName() + "' 持久化类");
                    }
                }
                else{
                    throw new AsyncDBException("找不到类'" + claxx.getName() + "' 持久化类");
                }
            }
            else{
                throw new AsyncDBException("找不到类'" + claxx.getName() + "' 持久化类");
            }
        }

        return class2Persistent.get(claxx);
    }

    boolean dbOpr(AsyncDBEntity asyncDBEntity, DBOperation operation){
        asyncDBEntity.serialize();
        try{
            DBSynchronzier DBSynchronzier = getAsyncPersistent(asyncDBEntity);

            if(DBSynchronzier != null){
                if(asyncDBEntity.getAsyncPersistent() == null){
                    asyncDBEntity.setAsyncPersistent(DBSynchronzier);
                }
                if(asyncDBEntity.isCanPersist(operation)){
                    asyncDBExecutor.submit(asyncDBEntity);

                    return true;
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public void close() {
        class2Persistent.clear();
        asyncDBExecutor.close();
    }
}
