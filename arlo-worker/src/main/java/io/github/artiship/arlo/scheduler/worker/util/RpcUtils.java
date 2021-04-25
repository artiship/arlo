package io.github.artiship.arlo.scheduler.worker.util;

import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.model.ZkLostTask;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import java.time.LocalDateTime;


@Slf4j
public class RpcUtils {


    public static void updateTask(ZkClientHolder zkClientHolder, RpcClientHolder rpcClientHolder, SchedulerTaskBo task, TaskState taskState, Long pid, LocalDateTime endtime) {
        Long taskId = task.getId();
        //任务在执行"KILL"指令, 执行的状态不进行返回
        if (taskState != TaskState.KILLED
                && taskState != TaskState.KILL_FAIL
                && CommonCache.isKillTask(taskId)) {
            log.info("Task_{}_{}, task is doing kill, do not return execute state.", task.getJobId(), taskId);
            return;
        }

        switch (taskState) {
            case RUNNING:
                task.setPid(pid);
                break;
            case KILLED:
            case FAIL:
            case SUCCESS:
                task.setEndTime(endtime);
                break;
            case KILL_FAIL:
                break;
        }

        task.setTaskState(taskState);

        try {
            send2Master(rpcClientHolder, task.toRpcTask());
        } catch (Exception e) {
            log.warn("update state to master failed, downgrade record to zookeeper.", e);
            //更新状态到Master失败, 把信息更新到ZK
            updateState2Zk(zkClientHolder, task);
        }
    }


    public static void updateState2Zk(ZkClientHolder zkClientHolder, SchedulerTaskBo task) {
        //对于非最终状态, 不需要更新ZK
        if (task.getTaskState()
                .getCode() != TaskState.RUNNING.getCode()) {
            ZkLostTask zkLostTask = ZkLostTask.of(task.getId(), task.getTaskState(), task.getEndTime());
            StringBuilder pathBuilder = new StringBuilder(GlobalConstants.LOST_TASK_GROUP);
            String basePath = pathBuilder.toString();
            //创建节点
            if (!ZkUtils.exists(zkClientHolder.getZkClient(), basePath)) {
                ZkUtils.createPathRecursive(zkClientHolder.getZkClient(), basePath, CreateMode.PERSISTENT);
            }

            pathBuilder.append(Constants.ZK_PATH_SEPARATOR)
                       .append(task.getId());
            ZkUtils.createNode(zkClientHolder.getZkClient(), pathBuilder.toString(), zkLostTask.toString(), CreateMode.PERSISTENT);
        }
    }


    public static void send2Master(RpcClientHolder rpcClientHolder, RpcTask rpcTask) throws Exception {
        rpcClientHolder.updateTask(rpcTask);
    }
}
