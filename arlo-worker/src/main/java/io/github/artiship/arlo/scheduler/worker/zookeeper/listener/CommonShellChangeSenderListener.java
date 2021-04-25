package io.github.artiship.arlo.scheduler.worker.zookeeper.listener;

import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.util.ZkWatcherSenderUtil;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;


@Slf4j
public class CommonShellChangeSenderListener implements IZkChildListener {

    private ZkClient zkClient;

    public CommonShellChangeSenderListener(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> nodes) throws Exception {
        if (null != nodes && 0 != nodes.size()) {
            ZkWatcherSenderUtil.sendSyncNode(parentPath, nodes, zkClient, Constants.FILE_SYNC_HANDLER_SOURCE_WATCHER);
        }
    }
}
