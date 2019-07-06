package org.kin.framework.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.kin.framework.exception.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by huangjianqin on 2019/6/18.
 */
public class HttpUtils {
    private static final Logger log = LoggerFactory.getLogger("http");
    private static final Charset UTF8 = Charset.forName("utf-8");
    //http
    private static final SocketConfig DEFAULT_HTTP_SOCKET_CONFIG = SocketConfig.custom().setTcpNoDelay(true)
            .setSoTimeout(3000).build();
    private static final RequestConfig DEFAULT_HTTP_REQUEST_CONFIG = RequestConfig.custom().setConnectTimeout(3000)
            .setConnectionRequestTimeout(3000).setSocketTimeout(3000).setMaxRedirects(3).build();
    private static final PoolingHttpClientConnectionManager HTTP_CONNECTION_MANAGER;
    private static final ResponseHandler<HttpResponseWrapper> DEFAULT_HTTP_RESPONSE_HANDLER = (httpResponse) -> {
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        String reasonPhrase = httpResponse.getStatusLine().getReasonPhrase();
        HttpEntity httpEntity = httpResponse.getEntity();
        if (200 <= statusCode && statusCode < 300) {
            if (httpEntity != null) {
                return new HttpResponseWrapper(statusCode, EntityUtils.toString(httpEntity, UTF8), reasonPhrase);
            } else {
                return new HttpResponseWrapper(statusCode);
            }
        } else {
            EntityUtils.consume(httpEntity);
            throw new HttpException(statusCode, reasonPhrase, httpResponse.toString());
        }
    };

    //异步http
    private static final PoolingNHttpClientConnectionManager ASYNC_HTTP_CONNECTION_MANAGER;

    static {
        PoolingHttpClientConnectionManager httpConnectionManager = new PoolingHttpClientConnectionManager();
        httpConnectionManager.setMaxTotal(200);
        httpConnectionManager.setDefaultMaxPerRoute(20);
        HTTP_CONNECTION_MANAGER = httpConnectionManager;


        ConnectingIOReactor ioReactor = null;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            log.error(e.getMessage(), e);
        }
        PoolingNHttpClientConnectionManager nHttpConnectionManager = new PoolingNHttpClientConnectionManager(ioReactor);
        nHttpConnectionManager.setMaxTotal(100);
        nHttpConnectionManager.setDefaultMaxPerRoute(50);
        ASYNC_HTTP_CONNECTION_MANAGER = nHttpConnectionManager;
    }

    public static class HttpResponseWrapper {
        private int statusCode;
        private String content;
        private String reasonPhrase;

        public HttpResponseWrapper(int statusCode, String content, String reasonPhrase) {
            this.statusCode = statusCode;
            this.content = content;
            this.reasonPhrase = reasonPhrase;
        }

        public HttpResponseWrapper(int statusCode) {
            this(statusCode, "", "");
        }

        public JSONObject json() {
            return JSON.parseObject(content);
        }

        public <T> T obj(Type type) {
            return JSON.parseObject(content, type);
        }

        public <T> T obj(Class<T> claxx) {
            return JSON.parseObject(content, claxx);
        }

        //getter
        public int getStatusCode() {
            return statusCode;
        }

        public String getContent() {
            return content;
        }

        public String getReasonPhrase() {
            return reasonPhrase;
        }

        @Override
        public String toString() {
            return "HttpResponseWrapper{" +
                    "statusCode=" + statusCode +
                    ", content='" + content + '\'' +
                    ", reasonPhrase='" + reasonPhrase + '\'' +
                    '}';
        }
    }

    public interface AsyncHttpCallback {
        void completed(HttpResponseWrapper wrapper);

        void failed(Exception e);

        void cancelled();
    }

    //-----------------------------------------------------------------------------------------------------
    public static Map<String, String> parseQueryStr(String query) {
        if (StringUtils.isNotBlank(query)) {
            String[] splits = query.split("&");
            Map<String, String> params = new HashMap<>(splits.length);
            for (String kv : splits) {
                String[] kvArr = kv.split("=");
                if (kvArr.length == 2) {
                    params.put(kvArr[0], kvArr[1]);
                } else {
                    params.put(kvArr[0], "");
                }
            }

            return params;
        }

        return Collections.emptyMap();
    }

    public static String toQueryStr(Map<String, Object> params) {
        if (CollectionUtils.isNonEmpty(params)) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                Object value = entry.getValue();
                Object[] values;
                if (value.getClass().isArray()) {
                    values = (Object[]) value;
                } else {
                    values = new Object[]{value};
                }

                for (Object item : values) {
                    sb.append(entry.getKey() + "=" + item.toString() + "&");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        return "";
    }

    //------------------------------------------http请求------------------------------------------------------
    private static HttpUriRequest setParams(HttpUriRequest request, String url, Map<String, Object> params) {
        if (CollectionUtils.isNonEmpty(params)) {
            if (request instanceof HttpEntityEnclosingRequestBase) {
                HttpEntityEnclosingRequestBase httpEntityEnclosingRequestBase = (HttpEntityEnclosingRequestBase) request;
                List<NameValuePair> nvps = new ArrayList<>();
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    Object value = entry.getValue();
                    Object[] values;
                    if (value.getClass().isArray()) {
                        values = (Object[]) value;
                    } else {
                        values = new Object[]{value};
                    }
                    for (Object item : values) {
                        nvps.add(new BasicNameValuePair(entry.getKey(), item.toString()));
                    }
                }

                httpEntityEnclosingRequestBase.setEntity(new UrlEncodedFormEntity(nvps, UTF8));
            } else if (request instanceof HttpGet) {
                HttpGet httpGet = (HttpGet) request;
                String paramStr = toQueryStr(params);
                if (url.indexOf("?") > 0) {
                    url = url + "&" + paramStr;
                } else {
                    url = url + "?" + paramStr;
                }
                httpGet.setURI(URI.create(url));
            }

        }

        return request;
    }

    public static CloseableHttpClient httpClient() {
        return HttpClientBuilder.create()
                .setConnectionManager(HTTP_CONNECTION_MANAGER)
                .setDefaultSocketConfig(DEFAULT_HTTP_SOCKET_CONFIG)
                .setDefaultRequestConfig(DEFAULT_HTTP_REQUEST_CONFIG)
                .build();
    }

    public static HttpResponseWrapper get(String url) {
        return get(url, Collections.emptyMap());
    }

    public static HttpResponseWrapper get(String url, Map<String, Object> params) {
        log.info("http get '{}', params: '{}'", url, params);
        HttpGet httpGet = new HttpGet(url);
        setParams(httpGet, url, params);
        try {
            return httpClient().execute(httpGet, DEFAULT_HTTP_RESPONSE_HANDLER);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static HttpResponseWrapper post(String url){
        return post(url, Collections.emptyMap());
    }

    public static HttpResponseWrapper post(String url, Map<String, Object> params) {
        log.info("http post '{}', params: '{}'", url, params);
        HttpPost httpPost = new HttpPost(url);
        setParams(httpPost, url, params);
        try {
            return httpClient().execute(httpPost, DEFAULT_HTTP_RESPONSE_HANDLER);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static HttpResponseWrapper put(String url){
        return put(url, Collections.emptyMap());
    }

    public static HttpResponseWrapper put(String url, Map<String, Object> params) {
        log.info("http put '{}', params: '{}'", url, params);
        HttpPut httpPut = new HttpPut(url);
        setParams(httpPut, url, params);
        try {
            return httpClient().execute(httpPut, DEFAULT_HTTP_RESPONSE_HANDLER);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static HttpResponseWrapper delete(String url){
        return delete(url, Collections.emptyMap());
    }

    public static HttpResponseWrapper delete(String url, Map<String, Object> params) {
        log.info("http delete '{}', params: '{}'", url, params);
        HttpDelete httpDelete = new HttpDelete(url);
        setParams(httpDelete, url, params);
        try {
            return httpClient().execute(httpDelete, DEFAULT_HTTP_RESPONSE_HANDLER);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static CloseableHttpAsyncClient asyncHttpClient() {
        return HttpAsyncClientBuilder.create()
                .setConnectionManager(ASYNC_HTTP_CONNECTION_MANAGER)
                .setDefaultRequestConfig(DEFAULT_HTTP_REQUEST_CONFIG)
                .build();
    }

    private static void stop(CloseableHttpAsyncClient httpAsyncClient){
        try {
            httpAsyncClient.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void asyncHttpRequest(String url, Map<String, Object> params, HttpUriRequest request, AsyncHttpCallback callback) {
        CloseableHttpAsyncClient httpAsyncClient = asyncHttpClient();
        httpAsyncClient.start();
        httpAsyncClient.execute(request, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    log.info("async http '{}', params: '{}' completed", url, params);
                    callback.completed(DEFAULT_HTTP_RESPONSE_HANDLER.handleResponse(httpResponse));
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                finally {
                    stop(httpAsyncClient);
                }
            }

            @Override
            public void failed(Exception e) {
                log.error("async http '{}', params: '{}' failed", url, params);
                stop(httpAsyncClient);
                callback.failed(e);
            }

            @Override
            public void cancelled() {
                log.info("async http '{}', params: '{}' cancelled", url, params);
                stop(httpAsyncClient);
                callback.cancelled();
            }
        });
    }

    public static void get(String url, AsyncHttpCallback callback) {
        get(url, Collections.emptyMap(), callback);
    }

    public static void get(String url, Map<String, Object> params, AsyncHttpCallback callback) {
        log.info("async http get '{}', params: '{}'", url, params);
        HttpGet httpGet = new HttpGet(url);
        setParams(httpGet, url, params);
        asyncHttpRequest(url, params, httpGet, callback);
    }

    public static void post(String url, AsyncHttpCallback callback) {
        post(url, Collections.emptyMap(), callback);
    }

    public static void post(String url, Map<String, Object> params, AsyncHttpCallback callback) {
        log.info("async http post '{}', params: '{}'", url, params);
        HttpPost httpPost = new HttpPost(url);
        setParams(httpPost, url, params);
        asyncHttpRequest(url, params, httpPost, callback);
    }

    public static void put(String url, AsyncHttpCallback callback) {
        put(url, Collections.emptyMap(), callback);
    }

    public static void put(String url, Map<String, Object> params, AsyncHttpCallback callback) {
        log.info("async http put '{}', params: '{}'", url, params);
        HttpPut httpPut = new HttpPut(url);
        setParams(httpPut, url, params);
        asyncHttpRequest(url, params, httpPut, callback);
    }

    public static void delete(String url, AsyncHttpCallback callback) {
        delete(url, Collections.emptyMap(), callback);
    }

    public static void delete(String url, Map<String, Object> params, AsyncHttpCallback callback) {
        log.info("async http delete '{}', params: '{}'", url, params);
        HttpDelete httpDelete = new HttpDelete(url);
        setParams(httpDelete, url, params);
        asyncHttpRequest(url, params, httpDelete, callback);
    }
}
