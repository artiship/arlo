package io.github.artiship.arlo.scheduler.worker.util;

import com.alibaba.fastjson.JSONArray;
import io.github.artiship.arlo.model.ZkFile;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

import java.util.List;


@Slf4j
public class ZkWatcherSenderUtil {

    public static void sendSyncNode(String basePath, List<String> nodes, ZkClient zkClient, String source) {
        synchronized (ZkWatcherSenderUtil.class) {
            for (String node : nodes) {
                StringBuilder lockPathBuilder = new StringBuilder();
                lockPathBuilder.append(Constants.SYNC_TASK)
                               .append(Constants.ZK_PATH_SEPARATOR)
                               .append(node);
                //创建分发节点, 避免被其他的worker多次分发
                boolean createSuccess = ZkUtils.createNodeIfNotExist(zkClient, lockPathBuilder.toString(), "", CreateMode.EPHEMERAL);
                if (createSuccess) {
                    log.info("send sync task, source : [{}], taskTag : [{}]", source, node);
                    //将任务同步到所有在线节点
                    StringBuilder syncTaskNode = new StringBuilder();
                    syncTaskNode.append(basePath)
                                .append(Constants.ZK_PATH_SEPARATOR)
                                .append(node);
                    try {
                        //同步任务信息
                        String syncTasksInfo = ZkUtils.getNodeData(zkClient, syncTaskNode.toString());
                        log.info("sync task info : [{}], source : [{}], taskTag : [{}]", syncTasksInfo, source, node);
                        if (null == syncTasksInfo) {
                            log.error("send sync task failed, task data : [{}], source : [{}], taskTag : [{}]", syncTasksInfo, source, node);
                            continue;
                        }

                        try {
                            JSONArray.parseArray(syncTasksInfo, ZkFile.class);
                        } catch (Exception e) {
                            log.error("send sync task failed, parse to json array failed, task data : [{}], source : [{}], taskTag : [{}]", syncTasksInfo, source, node);
                            continue;
                        }

                        List<String> allWorkers = ZkUtils.getWorkers(zkClient);
                        StringBuilder syncWorkerPathBuilder = new StringBuilder();
                        for (String worker : allWorkers) {
                            syncWorkerPathBuilder.setLength(0);
                            syncWorkerPathBuilder.append(Constants.SYNC_TASK_WORKER)
                                                 .append(Constants.ZK_PATH_SEPARATOR)
                                                 .append(worker)
                                                 .append(Constants.ZK_PATH_SEPARATOR)
                                                 .append(node);

                            //将同步任务同步到每个节点
                            if (ZkUtils.createNodeIfNotExist(zkClient, syncWorkerPathBuilder.toString(), syncTasksInfo,
                                    CreateMode.PERSISTENT)) {
                                log.info("send sync task [{}] to worker [{}] success, source : [{}].", node, worker, source);
                            } else {
                                log.error("send sync task [{}] to worker [{}] failed, source : [{}]", node, worker, source);
                            }
                        }
                    } finally {
                        //删除锁节点
                        ZkUtils.delNode(zkClient, lockPathBuilder.toString());
                        //删除ZK的任务节点
                        ZkUtils.delNode(zkClient, syncTaskNode.toString());
                    }
                }
            }
        }
    }
}
