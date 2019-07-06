package org.kin.framework.utils;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * @author huangjianqin
 * @date 2019/7/7
 */
public class HttpUtilsTest {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        HttpUtils.get("http://fanyi.youdao.com/translate_o", Collections.singletonMap("i", "word"), new HttpUtils.AsyncHttpCallback() {
            @Override
            public void completed(HttpUtils.HttpResponseWrapper wrapper) {
                System.out.println(wrapper.getContent());
                latch.countDown();
            }

            @Override
            public void failed(Exception e) {
                System.err.println(e);
            }

            @Override
            public void cancelled() {
                System.out.println("cancelled");
            }
        });
        System.out.println(HttpUtils.post("http://fanyi.youdao.com/translate_o", Collections.singletonMap("i", "word")).json());
        latch.await();
    }
}
