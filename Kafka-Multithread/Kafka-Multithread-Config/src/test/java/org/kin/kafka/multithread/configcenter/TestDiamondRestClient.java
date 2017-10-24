package org.kin.kafka.multithread.configcenter;

import org.junit.Test;
import org.kin.kafka.multithread.utils.HostUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;

/**
 * Created by huangjianqin on 2017/10/7.
 */
public class TestDiamondRestClient extends TestConfigBase{

//    @Test
    public void storeYAMLConfig(){
        String appName = "test";
        String host = HostUtils.localhost();

        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target(String.format("http://%s:60000/kafkamultithread/config/post/%s/%s", HostUtils.localhost(), appName, "yaml"));
        Form form = new Form();
        form.param("config", getYAMLConfig(properties));
        form.param("host", host);
        Response response = target.request().post(Entity.form(form));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        restClient.close();
    }

    @Test
    public void storeJSONConfig(){
        String appName = "test1";
        String host = HostUtils.localhost();

        Client restClient = ClientBuilder.newClient();
        System.out.println();
        WebTarget target = restClient.target(String.format("http://%s:60000/kafkamultithread/config/post/%s/%s", HostUtils.localhost(), appName, "json"));
        Form form = new Form();
        form.param("config", getJSONConfig(properties));
        form.param("host", host);
        Response response = target.request().post(Entity.form(form));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        restClient.close();
    }

//    @Test
    public void storePropertiesConfig(){
        String appName = "test";
        String host = HostUtils.localhost();

        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target(String.format("http://%s:60000/kafkamultithread/config/post/%s/%s", HostUtils.localhost(), appName, "properties"));
        Form form = new Form();
        form.param("config", getPropertiesStr(properties));
        form.param("host", host);
        Response response = target.request().post(Entity.form(form));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        restClient.close();
    }

//    @Test
    public void getPropertiesConfig(){
        String appName = "test";
        String host = HostUtils.localhost();
        String type = "properties";

        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target(String.format("http://%s:60000/kafkamultithread/config/get/%s/%s", HostUtils.localhost(), appName, type));
        Response response = target.queryParam("host", host).request().get();

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        restClient.close();
    }

//    @Test
    public void getJSONConfig(){
        String appName = "test1";
        String host = HostUtils.localhost();
        String type = "json";

        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target(String.format("http://%s:60000/kafkamultithread/config/get/%s/%s", HostUtils.localhost(), appName, type));
        Response response = target.queryParam("host", host).request().get();

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        restClient.close();
    }

//    @Test
    public void getYAMLConfig(){
        String appName = "test";
        String host = HostUtils.localhost();
        String type = "yaml";

        Client restClient = ClientBuilder.newClient();
        WebTarget target = restClient.target(String.format("http://%s:60000/kafkamultithread/config/get/%s/%s", HostUtils.localhost(), appName, type));
        Response response = target.queryParam("host", host).request().get();

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        restClient.close();
    }
}
