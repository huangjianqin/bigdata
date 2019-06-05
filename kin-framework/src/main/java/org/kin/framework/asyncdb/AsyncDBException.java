package org.kin.framework.asyncdb;

/**
 * Created by huangjianqin on 2019/4/3.
 */
public class AsyncDBException extends RuntimeException {
    public AsyncDBException() {
    }

    public AsyncDBException(String message) {
        super(message);
    }

    public AsyncDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public AsyncDBException(Throwable cause) {
        super(cause);
    }

    public AsyncDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
