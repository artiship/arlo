package io.github.artiship.arlo.scheduler.executor.zk;

import io.github.artiship.arlo.scheduler.core.rpc.api.RpcHeartbeat;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcResponse;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.scheduler.core.rpc.api.SchedulerServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcService extends SchedulerServiceGrpc.SchedulerServiceImplBase {


    @Override
    public void updateTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        rpcResponseBuilder.setCode(200);
        log.info("update task , rpctask [{}]", rpcTask.toString());
        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(RpcHeartbeat request, StreamObserver<RpcResponse> responseObserver) {
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        rpcResponseBuilder.setCode(200);
        log.info("heart beat , heartbeat [{}]", request.toString());
        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }
}
