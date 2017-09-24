package org.kin.kafka.multithread.distributed.container;

import org.kin.kafka.multithread.api.MultiThreadConsumerManager;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.core.Application;
import org.kin.kafka.multithread.distributed.AppStatus;
import org.kin.kafka.multithread.distributed.node.ContainerContext;
import org.kin.kafka.multithread.distributed.node.NodeContext;
import org.kin.kafka.multithread.domain.HealthReport;
import org.kin.kafka.multithread.protocol.distributed.ContainerMasterProtocol;
import org.kin.kafka.multithread.protocol.distributed.NodeMasterProtocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2017/9/12.
 * Application容器,多JVM运行,充分利用同一节点的计算和存储资源
 */
public abstract class Container implements ContainerMasterProtocol {
    protected long containerId;
    protected long idleTimeout;
    protected long reportInternal;
    protected int containerMasterProtocolPort;
    protected int nodeMasterProtocolPort;
    //所属NodeId
    protected long belong2;
    protected NodeMasterProtocol nodeMasterProtocol;

    private long lastCommunicateTime = System.currentTimeMillis();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Container scheduled thread");
            return thread;
        }
    });
    //在另外的线程执行配置更新
    private ExecutorService appStartPool = Executors.newFixedThreadPool(1);

    protected MultiThreadConsumerManager appManager = MultiThreadConsumerManager.instance();

    protected Container(ContainerContext containerContext, NodeContext nodeContext) {
        this.idleTimeout = containerContext.getIdleTimeout();
        this.containerMasterProtocolPort = containerContext.getProtocolPort();
        this.nodeMasterProtocolPort = nodeContext.getProtocolPort();
        this.reportInternal = containerContext.getReportInternal();
    }

    public abstract void doStart();
    public abstract void doClose();

    public void start(){
        //启动定时汇报心跳线程
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                healthReport();
            }
        }, 0, reportInternal, TimeUnit.MILLISECONDS);

        //定时检查是否空闲超时
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if(appManager.getAppSize() == 0){
                    if(System.currentTimeMillis() - lastCommunicateTime >= idleTimeout){
                        close();
                    }
                }
            }
        }, 0, idleTimeout, TimeUnit.MILLISECONDS);

        doStart();
    }

    public void close(){
        //先通知Node,但不关闭通信,为了避免再次分配Application到当前的Container,再自行关闭
        nodeMasterProtocol.closeContainer(containerId);
        scheduledExecutorService.shutdownNow();
        appStartPool.shutdownNow();
        doClose();
    }

    private void healthReport(){
        HealthReport healthReport = new HealthReport(appManager.getAppSize(), containerId);
        nodeMasterProtocol.report(healthReport);
    }

    @Override
    public Boolean updateConfig(List<Properties> configs) {
        lastCommunicateTime = System.currentTimeMillis();

        //考虑添加黑名单!!!!
        for(Properties config: configs){
            String appName = config.getProperty(AppConfig.APPNAME);

            AppStatus appStatus = AppStatus.getByStatusDesc(config.getProperty(AppConfig.APPSTATUS));
            //以后可能会根据返回值判断是否需要回滚配置更新
            Callable callable = null;
            switch (appStatus){
                case RUN:
                    if(!appManager.containsAppName(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                Application application = MultiThreadConsumerManager.instance().newApplication(config);
                                application.start();
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' already runned");
                    }
                    break;
                case UPDATE:
                    if(appManager.containsAppName(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                appManager.reConfig(config);
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
                case CLOSE:
                    if(!appManager.containsAppName(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                appManager.shutdownApp(appName);
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' already runned");
                    }
                    break;
                case RESTART:
                    if(appManager.containsAppName(appName)){
                        callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                appManager.shutdownApp(appName);
                                Application newApplication = MultiThreadConsumerManager.instance().newApplication(config);
                                newApplication.start();
                                return null;
                            }
                        };
                    }
                    else{
                        throw new IllegalStateException("app '" + appName + "' doesn't runned");
                    }
                    break;
            }
            appStartPool.submit(callable);
        }

        return true;
    }

    public long getContainerId() {
        return containerId;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getContainerMasterProtocolPort() {
        return containerMasterProtocolPort;
    }

    public int getNodeMasterProtocolPort() {
        return nodeMasterProtocolPort;
    }

    public long getBelong2() {
        return belong2;
    }
}
