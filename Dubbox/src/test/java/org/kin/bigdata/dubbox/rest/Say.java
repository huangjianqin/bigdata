package org.kin.bigdata.dubbox.rest;

import java.util.Map;

/**
 * Created by 健勤 on 2017/5/17.
 */
public interface Say {
    Map<String, String> say(String content, int id);
    void put(String content);
    void post(User user);
    void delete(User user);
}
