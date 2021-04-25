package io.github.artiship.arlo.scheduler.worker.executor;


import io.github.artiship.arlo.model.enums.JobType;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.executor.filecheck.FileCheckExecutor;
import io.github.artiship.arlo.scheduler.worker.executor.hive.HiveExecutor;
import io.github.artiship.arlo.scheduler.worker.executor.shell.ShellExecutor;
import io.github.artiship.arlo.scheduler.worker.executor.spark.SparkExecutor;
import io.github.artiship.arlo.scheduler.worker.executor.waterdrop.WaterdropExecutor;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;


public class ExecutorManager {

    private static Map<JobType, Class<? extends AbstractExecutor>> executors = new HashMap<>();

    static {
        executors.put(JobType.SHELL, ShellExecutor.class);
        executors.put(JobType.HQL, HiveExecutor.class);
        executors.put(JobType.SPARK, SparkExecutor.class);
        executors.put(JobType.MYSQL2HIVE, WaterdropExecutor.class);
        executors.put(JobType.FILE_CHECK, FileCheckExecutor.class);
    }

    public static AbstractExecutor getExecutor(SchedulerTaskBo task, RpcClientHolder rpcClientHolder,
                                               OSSClientHolder ossClientHolder, ZkClientHolder zkClientHolder)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        //任务类型
        JobType jobType = task.getJobType();
        Class<? extends AbstractExecutor> clazz = executors.get(jobType);
        //对于不能识别的JobType默认全部按照shell执行
        if (null == clazz) {
            clazz = ShellExecutor.class;
        }

        return clazz.getConstructor(SchedulerTaskBo.class, RpcClientHolder.class, OSSClientHolder.class, ZkClientHolder.class)
                    .newInstance(task, rpcClientHolder, ossClientHolder, zkClientHolder);
    }
}
