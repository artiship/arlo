package io.github.artiship.arlo.scheduler.manager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.model.enums.DagNodeType;
import io.github.artiship.arlo.model.enums.DagState;
import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.dag.DynamicJobDag;
import io.github.artiship.arlo.scheduler.manager.dag.DynamicTaskDag;
import io.github.artiship.arlo.scheduler.manager.dag.GenericDag;
import io.github.artiship.arlo.scheduler.manager.dag.StaticJobDag;
import io.github.artiship.arlo.scheduler.manager.dag.StaticTaskDag;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.artiship.arlo.model.enums.DagState.*;
import static io.github.artiship.arlo.model.enums.DagType.DYNAMIC;
import static io.github.artiship.arlo.model.enums.DagType.STATIC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.HOURS;

@Slf4j
@Component
public class DagScheduler implements Service {
    private final BlockingQueue<SchedulerDagBo> pendingDagQueue = new PriorityBlockingQueue<>();
    private final Map<Long, GenericDag> runningDagMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> batchParallelismMap = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> dagSuccessHistory = new ConcurrentHashMap<>();
    @Value("${services.dag-scheduler.thread.count:1}")
    private int schedulerThreadCount;
    @Value("${services.dag-restart.thread.count:1}")
    private int restartThreadCount;
    private ExecutorService dagSchedulerExecutor;
    private ExecutorService dagStartExecutor;

    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private TaskScheduler taskScheduler;
    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerService schedulerService;

    private Cache<Long, Long> toStopDags = CacheBuilder.newBuilder()
                                                       .expireAfterWrite(1, HOURS)
                                                       .build();

    public void advanceDag(SchedulerTaskBo task) {
        try {
            if (task == null || task.getDagId() == null) return;

            GenericDag genericDag = this.runningDagMap.get(task.getDagId());

            if (genericDag == null) return;

            genericDag.advance(task);

            if (genericDag.isCompleted()) {
                complete(genericDag);
                return;
            }
        } catch (Exception e) {
            this.finishDag(task.getDagId(), FAIL);
        }
    }

    public void stopDag(SchedulerTaskBo task) {
        try {
            if (task == null || task.getDagId() == null) return;

            this.finishDag(task.getDagId(), STOPPED);
        } catch (Exception e) {
            log.error("Dag_{} STOP fail", task.getDagId(), e);
        }

    }

    public void finishDag(Long dagId, DagState state) {
        GenericDag dag = runningDagMap.remove(dagId);
        if (dag != null) {
            dag.stop();
            this.schedulerDao.updateDagState(dagId, state);
            log.info("Dag_{} is {}", dag.getId(), state);
            return;
        }

        toStopDags.put(dagId, dagId);
    }

    private boolean tryStop(final SchedulerDagBo dag) {
        if (this.toStopDags.getIfPresent(dag.getId()) != null) {
            this.schedulerDao.updateDagState(dag.getId(), STOPPED);
            this.toStopDags.invalidate(dag.getId());

            log.info("Dag_{} STOPPED", dag.getId());
            return true;
        }

        return false;
    }

    public void complete(GenericDag genericDag) {
        this.schedulerDao.updateDagState(genericDag.getId(), COMPLETED);
        this.runningDagMap.remove(genericDag.getId());
        this.decrementParallelism(genericDag.getName());

        log.info("Dag_{} COMPLETE", genericDag.getId(), genericDag.getName());
    }

    public void submit(SchedulerDagBo dag) {
        pendingDagQueue.add(dag);
        this.schedulerDao.updateDagState(dag.getId(), PENDING);
        log.info("Dag_{} SUBMIT: start={}, schedule_time={}, dagType={}, nodeType={}",
                dag.getId(), dag.getStartId(), dag.getScheduleTime(), dag.getDagType(), dag.getDagNodeType());
    }

    private void finishDag(SchedulerDagBo dag, DagState state) {
        if (state == dag.getDagState()) return;
        this.schedulerDao.updateDagState(dag.getId(), state);
    }

    @Override
    public void start() throws Exception {
        this.dagStartExecutor = Executors.newFixedThreadPool(restartThreadCount);
        this.schedulerDao.getUncompletedDags(SUBMITTED, PENDING, LIMITED, RUNNING)
                         .forEach(dag -> runAsync(() -> {
                             if (dag.getDagState() == RUNNING) {
                                 GenericDag genericDag = createGenericDag(dag);
                                 log.info("Dag_{} RESTART: start={}, schedule_time={}, dagType={}, nodeType={}",
                                         dag.getId(), dag.getStartId(), dag.getScheduleTime(), dag.getDagType(), dag.getDagNodeType());
                                 genericDag.start();

                                 if (genericDag.isCompleted()) {
                                     complete(genericDag);
                                 }

                                 this.runningDagMap.putIfAbsent(dag.getId(), genericDag);
                                 this.incrementParallelism(dag.getDagName());
                                 return;
                             }

                             this.submit(dag);
                         }, dagStartExecutor).exceptionally(e -> {
                             log.error("Dag_{} reloaded and restart fail", dag.getId(), e);
                             this.schedulerDao.updateDagState(dag.getId(), FAIL);
                             return null;
                         }));

        this.dagSchedulerExecutor = Executors.newFixedThreadPool(schedulerThreadCount);
        this.dagSchedulerExecutor.submit(new DagSchedulerThread());
    }

    public int getParallelism(String dagName) {
        AtomicInteger atomicInteger = batchParallelismMap.get(dagName);
        if (atomicInteger == null) return 0;
        return atomicInteger.get();
    }

    public int incrementParallelism(String dagName) {
        AtomicInteger parallelism = batchParallelismMap.get(dagName);
        if (parallelism == null) {
            parallelism = new AtomicInteger(0);

            AtomicInteger pre = batchParallelismMap.putIfAbsent(dagName, parallelism);

            if (pre != null) parallelism = pre;
        }
        return parallelism.incrementAndGet();
    }

    public int decrementParallelism(String dagName) {
        AtomicInteger parallelism = batchParallelismMap.get(dagName);
        if (parallelism == null) return -1;

        if (parallelism.get() == 0) batchParallelismMap.remove(dagName);

        return parallelism.decrementAndGet();
    }

    public boolean isPreDagFinished(final SchedulerDagBo dag) {
        if (dag.isFirstOfTheBatch()) return true;
        return schedulerDao.isDagFinished(dag.getDagName(), dag.getBatchIndex() - 1);
    }

    public GenericDag createGenericDag(SchedulerDagBo schedulerDagBo) {
        requireNonNull(schedulerDagBo, "Scheduler Dag is null");

        if (schedulerDagBo.getDagType() == DYNAMIC) {
            if (schedulerDagBo.getDagNodeType() == DagNodeType.JOB) {
                return new DynamicJobDag(jobStateStore, schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
            }
            return new DynamicTaskDag(jobStateStore, schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
        }

        if (schedulerDagBo.getDagType() == STATIC) {
            if (schedulerDagBo.getDagNodeType() == DagNodeType.JOB) {
                return new StaticJobDag(schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
            }
            return new StaticTaskDag(schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
        }

        throw new RuntimeException("Dag type is not match");
    }

    @Override
    public void stop() throws Exception {
        if (dagSchedulerExecutor.isShutdown())
            return;

        dagSchedulerExecutor.shutdown();

        if (dagStartExecutor.isShutdown())
            return;

        dagStartExecutor.shutdown();
    }

    public Set<Long> getRunningDags() {
        return this.runningDagMap.keySet();
    }

    class DagSchedulerThread implements Runnable {

        @Override
        public void run() {
            while (true) {
                if (pendingDagQueue.isEmpty()) continue;

                try {
                    final SchedulerDagBo dag = pendingDagQueue.take();
                    try {
                        if (tryStop(dag)) continue;
                        if (getParallelism(dag.getDagName()) < dag.getParallelism()) {
                            if (dag.shouldRunSerial() && !isPreDagFinished(dag)) {
                                finishDag(dag, PENDING);
                                pendingDagQueue.add(dag.penalty());
                                continue;
                            }

                            GenericDag genericDag = createGenericDag(dag);
                            if (runningDagMap.putIfAbsent(dag.getId(), genericDag) == null) {

                                log.info("Dag_{} START: start={}, schedule_time={}, dagType={}, nodeType={}",
                                        dag.getId(), dag.getStartId(), dag.getScheduleTime(), dag.getDagType(), dag.getDagNodeType());

                                genericDag.start();
                                schedulerDao.updateDagState(dag.getId(), RUNNING);
                                continue;
                            }
                            log.warn("Dag_{} DISCARD.", dag.getId());
                            finishDag(dag.getId(), STOPPED);
                            continue;
                        }
                        finishDag(dag, LIMITED);
                    } catch (Exception e) {
                        pendingDagQueue.add(dag.penalty());
                        log.error("Dag_{} restart fail : start_id = {}", dag.getId(), dag.getStartId(), e);
                    }
                    pendingDagQueue.add(dag.penalty());
                } catch (Exception e) {
                    log.error("Dag scheduler thread interrupted.", e);
                }
            }
        }
    }
}
