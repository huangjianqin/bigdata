package org.kin.bigdata.dubbox.rest;

import com.alibaba.dubbo.rpc.RpcContext;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 健勤 on 2017/5/17.
 */
@Path("say")
public class SayImpl implements Say {
    @Override
    @GET
    //接受数据的格式
    @Consumes(MediaType.APPLICATION_JSON)
    //写入Response的格式
    @Produces(MediaType.APPLICATION_JSON)
    //URI路径
    @Path("{content: \\w+}/{id: \\d+}")
    public Map<String, String> say(@PathParam("content") String content, @PathParam("id") int id) {
        System.out.println(content + ":" + id);
        System.out.println(RpcContext.getContext().getRemoteAddressString());
        System.out.println("--------------------------------------------------------------------------");
        Map<String, String> result = new HashMap<>();
        result.put("content", content);
        result.put("id", String.valueOf(id));

        return result;
    }

    @Override
    @Path("put/{content: \\w+}")
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public void put(@PathParam("content") String content) {
        System.out.println("put: " + content);
    }

    @Override
    @Path("post/{name: \\w+}/{id: \\d+}")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void post(User user) {
        System.out.println("post: " + user);
    }

    @Override
    @Path("delete/{name: \\w+}/{id: \\d+}")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    public void delete(User user) {
        System.out.println("delete: " + user);
    }

}
