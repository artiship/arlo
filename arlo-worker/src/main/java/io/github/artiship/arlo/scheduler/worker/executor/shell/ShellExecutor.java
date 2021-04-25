package io.github.artiship.arlo.scheduler.worker.executor.shell;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.executor.waterdrop.WaterdropExecutor;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;

public class ShellExecutor extends WaterdropExecutor {

    private static final String SHELL_WORK_PATH = "/data/arlo/worker/log/%s/shell";

    public ShellExecutor(SchedulerTaskBo task, RpcClientHolder rpcClientHolder, OSSClientHolder ossClientHolder, ZkClientHolder zkClientHolder) {
        super(task, rpcClientHolder, ossClientHolder, zkClientHolder);
    }

    @Override
    public String getBaswWorkPath() {
        return SHELL_WORK_PATH;
    }
}
