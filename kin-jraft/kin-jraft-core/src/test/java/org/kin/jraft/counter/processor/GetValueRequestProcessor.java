package org.kin.jraft.counter.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.kin.jraft.counter.CounterClosure;
import org.kin.jraft.counter.CounterRaftService;
import org.kin.jraft.counter.message.GetValueRequest;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class GetValueRequestProcessor implements RpcProcessor<GetValueRequest> {

    private final CounterRaftService counterService;

    public GetValueRequestProcessor(CounterRaftService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, GetValueRequest request) {
        CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getResponse());
            }
        };

        this.counterService.get(request.isReadOnlySafe(), closure);
    }

    @Override
    public String interest() {
        return GetValueRequest.class.getName();
    }
}