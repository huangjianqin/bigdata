package org.kin.hbase.core.exception;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class HBaseEntityException extends RuntimeException {
    private static final long serialVersionUID = -9185747789260834478L;

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
