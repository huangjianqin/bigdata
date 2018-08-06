package org.kin.hbase.core.exception;

/**
 * Created by huangjianqin on 2018/5/27.
 */
public class HBaseEntityException extends RuntimeException {
    public HBaseEntityException() {
        super();
    }

    public HBaseEntityException(String message) {
        super(message);
    }

    public HBaseEntityException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseEntityException(Throwable cause) {
        super(cause);
    }

    protected HBaseEntityException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
