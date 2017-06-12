package org.microservice.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.SpringCloudApplication;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.zookeeper.discovery.ZookeeperInstance;
import org.springframework.cloud.zookeeper.serviceregistry.ServiceInstanceRegistration;
import org.springframework.cloud.zookeeper.serviceregistry.ZookeeperRegistration;
import org.springframework.cloud.zookeeper.serviceregistry.ZookeeperServiceRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 健勤 on 2017/5/16.
 */
//@SpringCloudApplication
//会识别@org.springframework.context.annotation.Configuration修饰的java配置类来加载bean
@SpringBootApplication
//可以把当前类注册为Zookeeper服务并发现Zookeeper上其他服务
@EnableDiscoveryClient
@RestController
public class Person {
    private long callCount = 0;

    @Resource
    private DiscoveryClient discoveryClient;

    @Resource
    private ZookeeperServiceRegistry zookeeperServiceRegistry;

    @RequestMapping(value = "manualregister", method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> manualRegister(){
        String host = "localhost";
        int port = 8080;
        String name = "manual";
        ZookeeperRegistration registration = ServiceInstanceRegistration.builder()
                .defaultUriSpec()
                //服务主机号
                .address(host)
                //服务端口
                .port(port)
                //服务名
                .name(name)
                //
                .payload(new ZookeeperInstance(name + ":" + port, name, Collections.singletonMap("instance_status", "UP")))
                .build();
        zookeeperServiceRegistry.register(registration);
        return Collections.singletonMap("result", "在节点[" + host + ":" + port + "]上的" + name + "服务注册成功!!!");
    }

    @RequestMapping(value = "say/{id}/{content}", method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> say(@PathVariable("id") Integer id, @PathVariable("content") String content){
        return Collections.singletonMap("callback", "user " + id + " say \"" + content + "\"");
    }

    @RequestMapping(value = "info", method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> info(){
        callCount++;
        //获取注册中心所有服务的一些元数据信息
        List<ServiceInstance> services = discoveryClient.getInstances("personService");
        Map<String, Object> servicesMap = new HashMap<>();
        if(services != null && services.size() > 0){
            servicesMap.put("size", services.size());
            if(services.size() > 0){
                for(ServiceInstance service: services) {
                    //获取服务id,所在主机号和端口
                    String key = service.getServiceId() + "[" + service.getHost() + ":" + service.getPort() + "]";
                    String value = "{"
                            + service.getUri() + ", "
                            + service.isSecure() + ", "
                            + service.getMetadata() +
                            "}";
                    servicesMap.put(key, value);
                }
            }
        }
        System.out.println(callCount);
        return servicesMap;
    }

    public DiscoveryClient getDiscoveryClient() {
        return discoveryClient;
    }

    public void setDiscoveryClient(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }

    public ZookeeperServiceRegistry getZookeeperServiceRegistry() {
        return zookeeperServiceRegistry;
    }

    public void setZookeeperServiceRegistry(ZookeeperServiceRegistry zookeeperServiceRegistry) {
        this.zookeeperServiceRegistry = zookeeperServiceRegistry;
    }

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = SpringApplication.run(Person.class, args);
//        new SpringApplicationBuilder(Test.class).web(true).run(args);
    }
}
