package org.kin.kafka.multithread.configcenter;

import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.utils.JsonUtils;
import org.kin.kafka.multithread.configcenter.utils.PropertiesUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by huangjianqin on 2017/10/7.
 */
public class RestClient {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(Object.class.getClassLoader().getResourceAsStream("appConfig.properties"));
        storeConfig(JsonUtils.map2Json(PropertiesUtils.properties2Map(properties)));
    }

    public static void storeConfig(String config){
        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target("http://192.168.40.1:60000/kafkamultithread/192.168.40.1/test/json/" + config);
        Response response = target.request().get();

        System.out.println(response.getStatus());
        restClient.close();
    }

    public static void getConfig(String appName, String host, String type){
        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target(String.format("http://192.168.40.1:60000/kafkamultithread/%s/%s/%s", appName, host, type));
        Response response = target.request().get();


        System.out.println(response.getStatus());

        restClient.close();
    }
}
