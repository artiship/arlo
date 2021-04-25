package io.github.artiship.arlo.scheduler.worker.thread;

import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.util.CommonUtils;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkUtils;
import io.github.artiship.arlo.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
@Slf4j
public class HandleMasterCommandThread implements Runnable {

    @Value("${arlo.worker.server.port}")
    private int workerPort;

    @Autowired
    private ZkClientHolder zkClientHolder;

    @Autowired
    private RpcClientHolder rpcClientHolder;

    @Override
    public void run() {
        try {
            List<String> deadNodes = zkClientHolder.getZkClient()
                                                   .getChildren(GlobalConstants.DEAD_WORKER_GROUP);
            if (null != deadNodes && 0 != deadNodes.size()) {
                String ipPort = ipPort();
                if (deadNodes.contains(ipPort)) {
                    log.info("Kill all task running on current node.");
                    //接到KILL所有运行在当前节点的指令
                    CommonUtils.killAllRunningTasks(zkClientHolder, rpcClientHolder, TaskState.KILLED);
                    //处理完成以后移除指令
                    removeMyself(ipPort);
                }
            }
        } catch (Exception e) {
            log.error("handle master command failed.", e);
        }
    }


    private String ipPort() {
        String ip = MetricsUtils.getHostIpAddress();
        StringBuilder nodeBuilder = new StringBuilder(ip);
        nodeBuilder.append(":")
                   .append(workerPort);
        return nodeBuilder.toString();
    }


    private void removeMyself(String ipPort) {
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append(GlobalConstants.DEAD_WORKER_GROUP)
                   .append(Constants.ZK_PATH_SEPARATOR)
                   .append(ipPort);
        ZkUtils.delNode(zkClientHolder.getZkClient(), pathBuilder.toString());
    }
}
