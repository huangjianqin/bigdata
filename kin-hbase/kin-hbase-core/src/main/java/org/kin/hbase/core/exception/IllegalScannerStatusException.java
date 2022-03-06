package org.kin.hbase.core.exception;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class IllegalScannerStatusException extends RuntimeException {
    private static final long serialVersionUID = -1658381733032200377L;

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
