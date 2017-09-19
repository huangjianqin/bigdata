package org.kin.kafka.multithread.distributed.container;

import org.kin.kafka.multithread.api.MultiThreadConsumerManager;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.core.Application;
import org.kin.kafka.multithread.distributed.AppStatus;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangjianqin on 2017/9/18.
 */
public class JVMContainer extends Container implements ContainerMasterProtocol{
    private ContainerMasterProtocol containerMasterProtocol;
    private NodeMasterProtocol nodeMasterProtocol;
    //在另外的线程执行配置更新
    private ExecutorService pool = Executors.newFixedThreadPool(1);

    public JVMContainer(ContainerContext containerContext, NodeContext nodeContext, NodeMasterProtocol nodeMasterProtocol) {
        super(containerContext, nodeContext);
        this.nodeMasterProtocol = nodeMasterProtocol;
    }

    @Override
    void start() {
        containerMasterProtocol = this;
    }

    @Override
    void close() {
        pool.shutdownNow();
    }

    @Override
    public Boolean updateConfig(List<Properties> configs) {
        //考虑添加黑名单!!!!
        for(Properties config: configs){
            String appName = config.getProperty(AppConfig.APPNAME);

            AppStatus appStatus = AppStatus.getByStatusDesc(config.getProperty(AppConfig.APPSTATUS));
            //以后可能会根据返回值判断是否需要回滚配置更新
            Callable callable = null;
            switch (appStatus){
                case RUN:
                    if(!appName2Application.containsKey(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                Application application = MultiThreadConsumerManager.instance().newApplication(config);
                                application.start();
                                appName2Application.put(appName, application);
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' already runned");
                    }
                    break;
                case UPDATE:
                    if(appName2Application.containsKey(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                appName2Application.get(appName).reConfig(config);
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
                case CLOSE:
                    if(!appName2Application.containsKey(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                appName2Application.get(appName).close();
                                appName2Application.remove(appName);
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' already runned");
                    }
                    break;
                case RESTART:
                    if(appName2Application.containsKey(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                appName2Application.get(appName).close();
                                Application newApplication = MultiThreadConsumerManager.instance().newApplication(config);
                                newApplication.start();
                                appName2Application.put(appName, newApplication);
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
            }
            pool.submit(callable);
        }

        return true;
    }

}
