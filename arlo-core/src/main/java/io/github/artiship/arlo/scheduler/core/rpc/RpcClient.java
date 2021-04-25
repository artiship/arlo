package io.github.artiship.arlo.scheduler.core.rpc;

import io.github.artiship.arlo.scheduler.core.exception.WorkerBusyException;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.rpc.api.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class RpcClient implements AutoCloseable {
    private ManagedChannel channel;
    private SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;

    private RpcClient(String host, Integer port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                                            .usePlaintext()
                                            .build();
        this.stub = SchedulerServiceGrpc.newBlockingStub(channel);
    }

    public static RpcClient create(String host, Integer port) {
        return new RpcClient(host, port);
    }

    public static RpcClient create(SchedulerNodeBo worker) {
        requireNonNull(worker, "Worker is null");
        requireNonNull(worker.getHost(), "Worker host is null");
        requireNonNull(worker.getPort(), "Worker port is null");

        return new RpcClient(worker.getHost(), worker.getPort());
    }

    public RpcResponse submitTask(RpcTask task) {
        RpcResponse response = stub.submitTask(task);
        if (response.getCode() == 500) {
            throw new WorkerBusyException(response.getMessage());
        }
        return response;
    }

    public RpcResponse killTask(RpcTask task) {
        return stub.killTask(task);
    }

    public RpcResponse updateTask(RpcTask task) {
        return stub.updateTask(task);
    }

    public RpcResponse heartbeat(RpcHeartbeat heartbeat) {
        return stub.heartbeat(heartbeat);
    }

    public HealthCheckResponse healthCheck(HealthCheckRequest request) {
        return stub.healthCheck(request);
    }

    public void shutdown() {
        try {
            if (channel.isShutdown())
                return;

            channel.shutdown()
                   .awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }
}
