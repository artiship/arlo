package io.github.artiship.arlo.scheduler.worker.executor.waterdrop;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.executor.AbstractExecutor;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WaterdropExecutor extends AbstractExecutor {

    private static final String WATERDROP_WORK_PATH = "/data/arlo/worker/log/%s/waterdrop";

    public WaterdropExecutor(SchedulerTaskBo task, RpcClientHolder rpcClientHolder, OSSClientHolder ossClientHolder, ZkClientHolder zkClientHolder) {
        super(task, rpcClientHolder, zkClientHolder, ossClientHolder);
    }

    @Override
    public String getBaswWorkPath() {
        return WATERDROP_WORK_PATH;
    }
}
