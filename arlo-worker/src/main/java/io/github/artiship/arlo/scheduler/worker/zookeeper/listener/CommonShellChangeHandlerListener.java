package io.github.artiship.arlo.scheduler.worker.zookeeper.listener;

import com.alibaba.fastjson.JSONArray;
import io.github.artiship.arlo.model.ZkFile;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.SyncCommonShellUtil;
import io.github.artiship.arlo.scheduler.worker.util.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.HashSet;
import java.util.List;


@Slf4j
public class CommonShellChangeHandlerListener implements IZkChildListener {

    private HashSet<String> processedSyncTasks = new HashSet<>();

    private ZkClient zkClient;

    private OSSClientHolder ossClientHolder;

    public CommonShellChangeHandlerListener(ZkClient zkClient, OSSClientHolder ossClientHolder) {
        this.zkClient = zkClient;
        this.ossClientHolder = ossClientHolder;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> syncTasks) throws Exception {
        if (null != syncTasks && 0 != syncTasks.size()) {
            synchronized (CommonShellChangeHandlerListener.class) {
                StringBuilder syncTaskPathBuilder = new StringBuilder();
                for (String task : syncTasks) {
                    syncTaskPathBuilder.setLength(0);
                    syncTaskPathBuilder.append(parentPath)
                                       .append(Constants.ZK_PATH_SEPARATOR)
                                       .append(task);
                    try {
                        //过滤已经处理的任务
                        if (!processedSyncTasks.contains(task)) {
                            log.info("handler sync task [{}]", task);
                            String syncTasksInfo = ZkUtils.getNodeData(zkClient, syncTaskPathBuilder.toString());
                            List<ZkFile> zkFiles = JSONArray.parseArray(syncTasksInfo, ZkFile.class);
                            for (ZkFile zkFile : zkFiles) {
                            }

                            processedSyncTasks.add(task);
                        } else {
                            log.warn("sync task with tag [{}] has processed. no need process again.", task);
                        }
                    } finally {
                        //删除任务节点
                        ZkUtils.delNode(zkClient, syncTaskPathBuilder.toString());
                    }
                }
            }
        }

    }
}
