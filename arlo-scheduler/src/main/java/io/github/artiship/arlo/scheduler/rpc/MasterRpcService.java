package io.github.artiship.arlo.scheduler.rpc;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcHeartbeat;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcResponse;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcResponse.Builder;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.scheduler.core.rpc.api.SchedulerServiceGrpc.SchedulerServiceImplBase;
import io.github.artiship.arlo.scheduler.manager.DagScheduler;
import io.github.artiship.arlo.scheduler.manager.JobStateStore;
import io.github.artiship.arlo.scheduler.manager.ResourceManager;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static io.github.artiship.arlo.model.enums.TaskState.finishStates;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.AUTO_RETRY;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.manualTypesExceptSingleCompletement;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo.from;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo.from;
import static io.github.artiship.arlo.scheduler.core.rpc.api.RpcResponse.newBuilder;

@Slf4j
@Service
public class MasterRpcService extends SchedulerServiceImplBase {

    @Autowired
    private ResourceManager resourceManager;
    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private DagScheduler dagScheduler;

    @Override
    public void heartbeat(RpcHeartbeat heartbeat, StreamObserver<RpcResponse> responseStreamObserver) {
        Builder response = newBuilder().setCode(200);

        log.debug("Receive worker {} heartbeat.", heartbeat.getHost());
        resourceManager.updateWorker(from(heartbeat));

        responseStreamObserver.onNext(response.build());
        responseStreamObserver.onCompleted();
    }

    @Override
    public void updateTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseStreamObserver) {
        Builder response = newBuilder().setCode(200);
        try {
            if (finishStates().contains(schedulerDao.getTaskStateById(rpcTask.getId()))) {
                return;
            }

            SchedulerTaskBo task = schedulerDao.saveTask(from(rpcTask));

            log.info("Task_{}_{} {} on worker {}: app_id={}, pid={}, dag={}",
                    task.getJobId(),
                    task.getId(),
                    task.getTaskState(),
                    task.getWorkerHost(),
                    task.getApplicationId(),
                    task.getPid(),
                    task.getDagId());

            switch (task.getTaskState()) {
                case RUNNING:
                    break;
                case SUCCESS:
                    jobStateStore.taskSuccess(task);
                    break;
                case FAIL:
                    jobStateStore.taskFailed(task);

                    if (task.retryable()) {
                        if (manualTypesExceptSingleCompletement().contains(task.getTaskTriggerType())) {
                            log.info("Task_{}_{} manual triggered task will not retry",
                                    task.getJobId(), task.getId(), task.getMaxRetryTimes());
                            break;
                        }

                        SchedulerTaskBo submit = taskDispatcher.submit(task.toRenewTask()
                                                                           .setTaskTriggerType(AUTO_RETRY));

                        log.info("Task_{}_{} retry, new task is Task_{}_{}, retry times is {}/{}.",
                                task.getJobId(), task.getId(), submit.getJobId(), submit.getId(), submit.getRetryTimes(), submit.getMaxRetryTimes());
                    }

                    log.info("Task_{}_{} retry times {} is exhausted", task.getJobId(), task.getId(), task.getMaxRetryTimes());

                    break;
                case KILLED:
                    jobStateStore.taskKilled(task);
                    dagScheduler.stopDag(task);
                    break;
                case FAILOVER:
                    jobStateStore.failoverTask(task);
                    SchedulerTaskBo submit = taskDispatcher.submit(task.toRenewTask()
                                                                       .setRetryTimes(task.getRetryTimes()));
                    log.info("Task_{}_{} is trying to failover to another worker, new task is Task_{}_{}.",
                            task.getJobId(), task.getId(), submit.getJobId(), submit.getId());
                default:
                    log.warn("Task_{}_{} state {} is not handled", task.getJobId(), task.getId(), task.getTaskState());
                    break;
            }
        } catch (Exception e) {
            log.warn("Task_{}_{} update fail", rpcTask.getJobId(), rpcTask.getId(), e);
        }

        responseStreamObserver.onNext(response.build());
        responseStreamObserver.onCompleted();
    }
}
