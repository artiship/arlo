package io.github.artiship.arlo.scheduler.worker.thread;

import io.github.artiship.arlo.scheduler.core.rpc.api.RpcHeartbeat;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkUtils;
import io.github.artiship.arlo.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static io.github.artiship.arlo.utils.Dates.protoTimestamp;
import static java.time.LocalDateTime.now;


@Slf4j
@Component
public class HeartbeatThread implements Runnable {

    //worker监听端口
    @Value("${arlo.worker.server.port}")
    private int workerPort;

    @Value("${arlo.worker.group}")
    private String groups;

    @Value("${arlo.worker.max.task.num}")
    private int maxTask;

    @Autowired
    private RpcClientHolder rpcClientHolder;

    @Autowired
    private ZkClientHolder zkClientHolder;

    @Autowired
    private OSSClientHolder ossClientHolder;

    @Override
    public void run() {
        //心跳对象
        RpcHeartbeat rpcHeartbeat = null;
        try {
            rpcHeartbeat = heartbeat();
            //发送心跳
            rpcClientHolder.heartbeat(rpcHeartbeat);
        } catch (Exception e) {
            log.error(String.format("send heartbeat failed. hearbeat info [%s]", rpcHeartbeat), e);
        }

        //如果注册信息不存在(ZK上面), 注册worker
        ZkUtils.regist(workerPort, zkClientHolder.getZkClient(), ossClientHolder, true);
    }


    private RpcHeartbeat heartbeat() {
        return RpcHeartbeat.newBuilder()
                           .setHost(MetricsUtils.getHostIpAddress())
                           .setPort(workerPort)
                           .setMaxTask(maxTask)
                           .setRunningTasks(CommonCache.currTaskCnt())
                           .setCpuUsage(MetricsUtils.getCpuUsage())
                           .setMemoryUsage(MetricsUtils.getMemoryUsage())
                           .setTime(protoTimestamp(now()))
                           .addAllWorkerGroups(Arrays.asList(groups.split(",")))
                           .build();
    }
}
