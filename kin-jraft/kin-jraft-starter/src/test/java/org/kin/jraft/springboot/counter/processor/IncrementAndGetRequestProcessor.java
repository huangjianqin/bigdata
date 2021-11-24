package org.kin.jraft.springboot.counter.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.kin.jraft.springboot.counter.message.IncrementAndGetRequest;
import org.kin.jraft.springboot.counter.server.CounterClosure;
import org.kin.jraft.springboot.counter.server.CounterRaftService;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class IncrementAndGetRequestProcessor implements RpcProcessor<IncrementAndGetRequest> {

    private final CounterRaftService counterService;

    public IncrementAndGetRequestProcessor(CounterRaftService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, IncrementAndGetRequest request) {
        CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getResponse());
            }
        };

        this.counterService.incrementAndGet(request.getDelta(), closure);
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}