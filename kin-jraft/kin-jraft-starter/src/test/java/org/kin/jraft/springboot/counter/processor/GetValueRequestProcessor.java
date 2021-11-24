package org.kin.jraft.springboot.counter.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.kin.jraft.springboot.counter.message.GetValueRequest;
import org.kin.jraft.springboot.counter.server.CounterClosure;
import org.kin.jraft.springboot.counter.server.CounterRaftService;

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