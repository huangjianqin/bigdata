package org.kin.microservice.springboot.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;

/**
 * Created by 健勤 on 2017/6/3.
 */
@SpringBootApplication
@EnableDiscoveryClient
@RestController
public class RestClient {
    @LoadBalanced
    @Bean
    public RestTemplate loadBalacneTemplate(){
        return new RestTemplate();
    }

    @Bean
    public RestTemplate restTemplate(){
        return new RestTemplate();
    }

    @Resource
    private RestTemplate loadBalacneTemplate;

    @Resource
    private LoadBalancerClient loadBalancerClient;

    @RequestMapping(value = "loadbalance", method = RequestMethod.GET)
    public String info(){
//        return loadBalacneTemplate.getForObject("http://personService/info", String.class) + "\r\n";
        //仅仅是选择,而不会发送请求
        //负载均衡地发送请求可能会首先调用该方法
        ServiceInstance instance = loadBalancerClient.choose("personService");
        return instance.getHost() + ":" + instance.getPort();
    }

    public static void main(String[] args) {
        SpringApplication.run(RestClient.class, args);
    }
}


