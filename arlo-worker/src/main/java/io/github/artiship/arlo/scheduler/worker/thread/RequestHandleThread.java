package io.github.artiship.arlo.scheduler.worker.thread;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.executor.AbstractExecutor;
import io.github.artiship.arlo.scheduler.worker.executor.ExecutorManager;
import io.github.artiship.arlo.scheduler.worker.model.OperatorType;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;

@Slf4j
public class RequestHandleThread implements Runnable {
    private RpcClientHolder rpcClientHolder;
    private OSSClientHolder ossClientHolder;
    private ZkClientHolder zkClientHolder;
    private SchedulerTaskBo schedulerTaskBo;
    private OperatorType operatorType;

    public RequestHandleThread(RpcClientHolder rpcClientHolder, OSSClientHolder ossClientHolder, SchedulerTaskBo schedulerTaskBo,
                               OperatorType operatorType, ZkClientHolder zkClientHolder) {
        this.rpcClientHolder = rpcClientHolder;
        this.ossClientHolder = ossClientHolder;
        this.zkClientHolder = zkClientHolder;
        this.schedulerTaskBo = schedulerTaskBo;
        this.operatorType = operatorType;
    }

    @Override
    public void run() {
        try {
            AbstractExecutor executor = getExecutor();
            switch (operatorType) {
                case SUBMIT:
                    executor.execute();
                    break;
                case KILL:
                    executor.kill();
                    break;
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            if (OperatorType.SUBMIT == operatorType) {
                CommonCache.delOneTask();
            }
        }
    }

    private AbstractExecutor getExecutor()
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return ExecutorManager.getExecutor(schedulerTaskBo, rpcClientHolder, ossClientHolder, zkClientHolder);
    }
}
