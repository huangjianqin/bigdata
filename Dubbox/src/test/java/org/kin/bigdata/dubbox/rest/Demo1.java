package org.kin.bigdata.dubbox.rest;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

/**
 * Created by 健勤 on 2017/5/17.
 */
public class Demo1 {
    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = new ClassPathXmlApplicationContext("services.xml");
//        while(true){}
        Thread.sleep(2000);
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target("http://localhost:9090/say/aa/1.json");
        Response response = target.request().get();

        try{
            if(response.getStatus() == 200){
                System.out.println(response.readEntity(String.class));
            }
            else{
                System.out.println(response.getStatus());
                System.out.println(response.getStatusInfo());
            }
        }finally {
            response.close();
        }

//        Thread.sleep(2000);
//        ApplicationContext context1 = new ClassPathXmlApplicationContext("consumers.xml");
//        Say consumer = (Say)context1.getBean("sayConsumer");
//        System.out.println(consumer.say("hahaha", 1));

    }
}
