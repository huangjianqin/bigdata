package org.kin.jraft;

import com.alipay.sofa.jraft.Closure;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public abstract class AbstractClosure<R extends AbstractResponse> implements Closure {
    /** response */
    protected R response;

    protected void failure(String errorMsg, String redirect) {
        response = createResponse();
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
    }

    protected void success() {
        response = createResponse();
        response.setSuccess(true);
    }

    protected abstract R createResponse();

    //setter && getter
    public R getResponse() {
        return response;
    }

    public void setResponse(R response) {
        this.response = response;
    }
}
