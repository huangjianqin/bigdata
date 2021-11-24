package org.kin.jraft.springboot.counter.message;

import org.kin.jraft.AbstractResponse;

/**
 * 返回值
 *
 * @author huangjianqin
 * @date 2021/11/14
 */
public class ValueResponse extends AbstractResponse {
    private static final long serialVersionUID = -8189962647998973008L;

    /** 值 */
    private long value;

    public ValueResponse(long value, boolean success, String redirect, String errorMsg) {
        super();
        this.value = value;
        setSuccess(success);
        setRedirect(redirect);
        setErrorMsg(errorMsg);
    }

    public ValueResponse() {
        super();
    }

    //setter && getter
    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return "ValueResponse{" +
                "value=" + value +
                "} " + super.toString();
    }
}
