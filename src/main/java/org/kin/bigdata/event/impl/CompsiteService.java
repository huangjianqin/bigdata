package org.kin.bigdata.event.impl;

import org.kin.bigdata.service.AbstractService;
import org.kin.bigdata.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by 健勤 on 2017/8/11.
 */
public class CompsiteService extends AbstractService {
    private static Logger log = LoggerFactory.getLogger(CompsiteService.class);
    private List<Service> services = new LinkedList<>();

    public CompsiteService(String serviceName) {
        super(serviceName);
    }

    @Override
    protected void serviceInit() {
        for(Service service: services){
            if(service.getCurrentState() == State.NOTINITED){
                service.init();
            }
            else{
                log.warn(service.toString() + " has inited or started");
            }
        }

        super.serviceInit();
    }

    @Override
    protected void serviceStart() {
        for(Service service: services){
            if(service.getCurrentState() == State.INITED){
                service.start();
            }
        }

        super.serviceStart();
    }

    @Override
    protected void serviceStop() {
        for(Service service: services){
            //初始化或启动后的服务就可以关闭
            if(service.getCurrentState() == State.INITED || service.getCurrentState() == State.STARTED){
                service.stop();
            }
        }
        super.serviceStop();
    }

    public void addService(Service service){
        synchronized (services){
            services.add(service);
        }
    }

    public boolean addIfService(Object mayBeService){
        if(mayBeService instanceof Service){
            addService((Service)mayBeService);
            return true;
        }
        else{
            return false;
        }
    }

    public boolean removeService(Service service){
        synchronized (services){
            return services.remove(service);
        }
    }

    public void addShutdownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread(new CompsiteServiceShutdownHook(this)));
    }

    /**
     * 关闭CompsiteService的钩子
     */
    private class CompsiteServiceShutdownHook implements Runnable{
        private CompsiteService service;

        public CompsiteServiceShutdownHook(CompsiteService service) {
            this.service = service;
        }

        @Override
        public void run() {
            service.serviceStop();
        }
    }
}
