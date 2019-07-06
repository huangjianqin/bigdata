package org.kin.framework.exception;

/**
 * @author huangjianqin
 * @date 2019/7/6
 */
public class HttpException extends RuntimeException {
    public HttpException(int statusCode, String reasonPhrase, String objStr) {
        super("http error!!! code=" + statusCode + ", reason=" + reasonPhrase + " >>>" + objStr);
    }
}
