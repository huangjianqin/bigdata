package org.kin.hbase.core.exception;

/**
 * Created by huangjianqin on 2018/5/27.
 */
public class IllegalScannerStatusException extends RuntimeException {
    public IllegalScannerStatusException() {
        super();
    }

    public IllegalScannerStatusException(String message) {
        super(message);
    }

    public IllegalScannerStatusException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalScannerStatusException(Throwable cause) {
        super(cause);
    }

    protected IllegalScannerStatusException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
