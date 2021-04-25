package io.github.artiship.arlo.scheduler.worker.rpc;

import com.google.common.base.Throwables;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.api.*;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.model.OperatorType;
import io.github.artiship.arlo.scheduler.worker.thread.RequestHandleThread;
import io.github.artiship.arlo.scheduler.worker.thread.ThreadPoolFactory;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;


@Component
@Slf4j
public class WorkerRpcServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase implements InitializingBean {

    @Autowired
    private RpcClientHolder rpcClientHolder;

    @Autowired
    private OSSClientHolder ossClientHolder;

    @Autowired
    private ZkClientHolder zkClientHolder;

    @Value("${arlo.worker.max.task.num}")
    private int maxTaskNums;

    private ThreadPoolExecutor threadPoolExecutor;

    @Override
    public void afterPropertiesSet() throws Exception {
        threadPoolExecutor = ThreadPoolFactory.create("worker-request-handler-pool", maxTaskNums * 3);
    }

    @Override
    public void submitTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        log.info("Get submit task request, rpcTask [{}]", rpcTask);
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        SchedulerTaskBo schedulerTaskBo = null;
        try {
            schedulerTaskBo = SchedulerTaskBo.from(rpcTask);
            submitTask(schedulerTaskBo);
            rpcResponseBuilder.setCode(200);
            CommonCache.addOneTask();
        } catch (Exception e) {
            rpcResponseBuilder.setCode(500)
                              .setMessage(subErrorMsg(e));
            log.error(String.format("Task_%s_%s, handle submit task request failed.", rpcTask.getJobId(), rpcTask.getId()), e);
        }

        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }


    @Override
    public void killTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        log.info("Get kill task request, rpcTask [{}]", rpcTask);
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        SchedulerTaskBo schedulerTaskBo = null;
        try {
            schedulerTaskBo = SchedulerTaskBo.from(rpcTask);
            Future future = killTask(schedulerTaskBo);
            future.get();
            rpcResponseBuilder.setCode(200);
        } catch (Exception e) {
            rpcResponseBuilder.setCode(500)
                              .setMessage(subErrorMsg(e));
            log.error(String.format("Task_%s_%s, handle kill task request failed.", rpcTask.getJobId(), rpcTask.getId()), e);
        }

        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        HealthCheckResponse healthCheckResponse = HealthCheckResponse.newBuilder()
                                                                     .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                                                                     .build();
        responseObserver.onNext(healthCheckResponse);
        responseObserver.onCompleted();
    }


    private void submitTask(SchedulerTaskBo task) {
        RequestHandleThread requestHandleThread = new RequestHandleThread(rpcClientHolder, ossClientHolder, task, OperatorType.SUBMIT, zkClientHolder);
        threadPoolExecutor.submit(requestHandleThread);
    }


    public Future killTask(SchedulerTaskBo task) {
        RequestHandleThread requestHandleThread = new RequestHandleThread(rpcClientHolder, ossClientHolder, task, OperatorType.KILL, zkClientHolder);
        return threadPoolExecutor.submit(requestHandleThread);
    }


    private String subErrorMsg(Exception e) {
        String allErrorMsg = Throwables.getStackTraceAsString(e);
        return allErrorMsg.length() > 50 ? allErrorMsg.substring(0, 50) : allErrorMsg;
    }
}