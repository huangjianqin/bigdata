package org.kin.jraft;

import java.io.Serializable;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public abstract class AbstractResponse implements Serializable {
    private static final long serialVersionUID = 4112600006937722778L;

    /** 请求是否成功 */
    private boolean success;
    /**
     * 重定向peer id
     * redirect peer id
     */
    private String redirect;
    /** 错误消息 */
    private String errorMsg;

    //setter && getter
    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getRedirect() {
        return this.redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "Response{" +
                "success=" + success +
                ", redirect='" + redirect + '\'' +
                ", errorMsg='" + errorMsg + '\'' +
                '}';
    }
}
