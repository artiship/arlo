package io.github.artiship.arlo.scheduler.worker.executor.filecheck;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.executor.waterdrop.WaterdropExecutor;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;

public class FileCheckExecutor extends WaterdropExecutor {

    private static final String FILECHECK_WORK_PATH = "/data/arlo/worker/log/%s/filecheck";

    public FileCheckExecutor(SchedulerTaskBo task, RpcClientHolder rpcClientHolder, OSSClientHolder ossClientHolder, ZkClientHolder zkClientHolder) {
        super(task, rpcClientHolder, ossClientHolder, zkClientHolder);
    }

    @Override
    public String getBaswWorkPath() {
        return FILECHECK_WORK_PATH;
    }
}
