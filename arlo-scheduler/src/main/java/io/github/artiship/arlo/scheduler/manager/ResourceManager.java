package io.github.artiship.arlo.scheduler.manager;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import io.github.artiship.arlo.model.ZkWorker;
import io.github.artiship.arlo.model.enums.NodeState;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.RpcClient;
import io.github.artiship.arlo.scheduler.core.rpc.api.HealthCheckRequest;
import io.github.artiship.arlo.scheduler.core.rpc.api.HealthCheckResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.rholder.retry.StopStrategies.stopAfterDelay;
import static com.github.rholder.retry.WaitStrategies.incrementingWait;
import static com.google.common.collect.ImmutableList.copyOf;
import static io.github.artiship.arlo.model.enums.NodeState.*;
import static io.github.artiship.arlo.scheduler.core.rpc.RpcClient.create;
import static io.github.artiship.arlo.scheduler.manager.WorkerSelector.builder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
public class ResourceManager {
    private final Map<String, SchedulerNodeBo> workers = new ConcurrentHashMap<>();

    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;

    private Retryer<Boolean> healthCheckRetryer = RetryerBuilder.<Boolean>newBuilder()
            .retryIfResult(Predicates.isNull())
            .retryIfExceptionOfType(Exception.class)
            .retryIfRuntimeException()
            .withWaitStrategy(incrementingWait(100, MILLISECONDS, 10, SECONDS))
            .withStopStrategy(stopAfterDelay(1, TimeUnit.MINUTES))
            .build();

    private ExecutorService healthCheckExecutor = Executors.newFixedThreadPool(10);


    public Optional<SchedulerNodeBo> availableWorker(SchedulerTaskBo task) {
        return builder().withWorkers(copyOf(workers.values()))
                        .withLastFailedHosts(jobStateStore.getFailedHosts(task.getJobId()))
                        .withTask(task)
                        .select();
    }

    public List<SchedulerNodeBo> getWorkers() {
        return ImmutableList.copyOf(this.workers.values());
    }

    public void updateWorker(SchedulerNodeBo worker) {
        if (worker == null)
            return;

        workers.put(worker.getHost(), schedulerDao.saveNode(worker));
    }

    public SchedulerNodeBo shutdownWorker(String workerHost) {
        return setWorkerState(workerHost, SHUTDOWN);
    }

    public SchedulerNodeBo resumeWorker(String workerHost) {
        return setWorkerState(workerHost, ACTIVE);
    }

    public void unHealthyWorker(String host) {
        final SchedulerNodeBo worker = setWorkerState(host, UN_HEALTHY);

        runAsync(() -> {
            try {
                if (healthCheckRetryer.call(() -> healthCheck(worker))) {
                    setWorkerState(host, ACTIVE);
                    log.info("Worker_{} recovery from an un healthy state", host);
                }
            } catch (Exception e) {
                setWorkerState(host, DEAD);
                log.info("Worker_{} is dead after a few times of retrying", host);
            }
        }, healthCheckExecutor);
    }

    private SchedulerNodeBo setWorkerState(String host, NodeState state) {
        SchedulerNodeBo worker = workers.get(host);
        if (worker != null) {
            worker.setNodeState(state);
            schedulerDao.saveNode(worker);
        }

        log.info("Worker_{} is {}.", host, state);

        return worker;
    }

    private Boolean healthCheck(final SchedulerNodeBo worker) throws Exception {
        requireNonNull(worker);

        try (final RpcClient workerRpcClient = create(worker.getHost(), worker.getPort())) {
            HealthCheckRequest request = HealthCheckRequest.newBuilder()
                                                           .setService("")
                                                           .build();

            HealthCheckResponse healthCheckResponse = workerRpcClient.healthCheck(request);

            if (healthCheckResponse.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
                return true;
            }
        }

        return false;
    }

    public void activeWorker(ZkWorker zkWorker) {
        SchedulerNodeBo worker = schedulerDao.saveNode(new SchedulerNodeBo().setHost(zkWorker.getIp())
                                                                            .setPort(zkWorker.getPort())
                                                                            .setNodeState(ACTIVE));

        SchedulerNodeBo schedulerNodeBo = workers.putIfAbsent(worker.getHost(), worker);

        if (schedulerNodeBo != null) {
            log.info("Worker_{} is transit from {} to active.",
                    zkWorker.toString(), schedulerNodeBo.getNodeState());
            schedulerNodeBo.setNodeState(ACTIVE);
        }

        log.info("Worker_{} is added to resource manager.", zkWorker.toString());
    }

    public void removeWorker(ZkWorker zkWorker) {
        workers.remove(zkWorker.getIp());
        schedulerDao.updateWorkerDead(zkWorker.getIp(), zkWorker.getPort());

        log.info("Worker_{} is removed from resource manager.", zkWorker.toString());
    }

    public void decrease(String host) {
        SchedulerNodeBo worker = workers.get(host);
        if (worker == null) return;
        synchronized (worker) {
            worker.setRunningTasks(worker.getRunningTasks() + 1);
        }
    }
}
