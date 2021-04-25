package io.github.artiship.arlo.scheduler.worker.executor.hive;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.executor.waterdrop.WaterdropExecutor;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveExecutor extends WaterdropExecutor {

    private static final String HIVE_WORK_PATH = "/data/arlo/worker/log/%s/hive";

    public HiveExecutor(SchedulerTaskBo task, RpcClientHolder rpcClientHolder, OSSClientHolder ossClientHolder, ZkClientHolder zkClientHolder) {
        super(task, rpcClientHolder, ossClientHolder, zkClientHolder);
    }

    @Override
    public String getBaswWorkPath() {
        return HIVE_WORK_PATH;
    }
}
