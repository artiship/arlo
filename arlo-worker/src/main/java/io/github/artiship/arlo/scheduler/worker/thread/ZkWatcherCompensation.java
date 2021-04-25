package io.github.artiship.arlo.scheduler.worker.thread;

import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkWatcherSenderUtil;

import java.util.List;


public class ZkWatcherCompensation implements Runnable {

    private ZkClientHolder zkClientHolder;

    public ZkWatcherCompensation(ZkClientHolder zkClientHolder) {
        this.zkClientHolder = zkClientHolder;
    }

    @Override
    public void run() {
        List<String> fileNodes = zkClientHolder.getZkClient()
                                               .getChildren(GlobalConstants.ZK_FILES_GROUP);
        if (null != fileNodes && 0 != fileNodes.size()) {
            ZkWatcherSenderUtil.sendSyncNode(GlobalConstants.ZK_FILES_GROUP, fileNodes, zkClientHolder.getZkClient(), Constants.FILE_SYNC_HANDLER_SOURCE_COMPENSATION);
        }

    }
}
