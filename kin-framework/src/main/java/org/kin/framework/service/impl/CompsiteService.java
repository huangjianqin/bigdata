package org.kin.framework.service.impl;


import org.kin.framework.service.AbstractService;
import org.kin.framework.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 健勤 on 2017/8/11.
 * 组合服务,也就是说该类或继承该类的子类会拥有许多服务
 */
public class CompsiteService extends AbstractService {
    private static Logger log = LoggerFactory.getLogger(CompsiteService.class);
    private List<Service> services = new LinkedList<>();

    public CompsiteService(String serviceName) {
        super(serviceName);
    }

    @Override
    protected void serviceInit() {
        for (Service service : services) {
            if (service.getCurrentState() == State.NOTINITED) {
                service.init();
            } else {
                log.warn(service.toString() + " state is " + service.getCurrentState());
            }
        }

        super.serviceInit();
    }

    @Override
    protected void serviceStart() {
        for (Service service : services) {
            if (service.getCurrentState() == State.INITED) {
                service.start();
            } else {
                log.warn(service.toString() + " state is " + service.getCurrentState());
            }
        }

        super.serviceStart();
    }

    @Override
    protected void serviceStop() {
        for (Service service : services) {
            //初始化或启动后的服务就可以关闭
            if (service.getCurrentState() == State.INITED || service.getCurrentState() == State.STARTED) {
                service.stop();
            } else {
                log.warn(service.toString() + " state is " + service.getCurrentState());
            }
        }
        super.serviceStop();
    }

    public void addService(@Nonnull Service service) {
        synchronized (services) {
            services.add(service);
        }
    }

    public boolean addIfService(Object mayBeService) {
        if (mayBeService instanceof Service) {
            addService((Service) mayBeService);
            return true;
        } else {
            return false;
        }
    }

    public boolean removeService(@Nonnull Service service) {
        synchronized (services) {
            return services.remove(service);
        }
    }
}
