package io.github.artiship.arlo.scheduler.worker.util;

import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.scheduler.core.rpc.RpcClient;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcHeartbeat;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.scheduler.worker.model.WorkerNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;


@Component
@Slf4j
public class RpcClientHolder implements InitializingBean, DisposableBean {

    @Autowired
    private ZkClientHolder zkClientHolder;

    private RpcClient rpcClient;

    //当前连接的master节点
    private WorkerNode activeMasterNode;

    @Override
    public void afterPropertiesSet() throws Exception {
        while (true) {
            try {
                WorkerNode masterNode = ZkUtils.activeMaster(zkClientHolder.getZkClient(), GlobalConstants.MASTER_GROUP);
                initRpcClient(masterNode);
                break;
            } catch (Exception e) {
                log.error("can not init rpc client.", e);
            }

            try {
                TimeUnit.SECONDS.sleep(3L);
            } catch (Exception e) {
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        if (null != rpcClient) {
            try {
                rpcClient.shutdown();
            } catch (Exception e) {
            }
        }
    }


    public RpcClient getRpcClient() {
        WorkerNode masterNode = null;
        try {
            masterNode = ZkUtils.activeMaster(zkClientHolder.getZkClient(), GlobalConstants.MASTER_GROUP);
        } catch (Exception e) {
            log.error(String.format("Get active master fail, use old master, [%s]", masterNode), e);
        }

        if (null != masterNode && !masterNode.equals(activeMasterNode)) {
            synchronized (RpcClientHolder.class) {
                if (null != masterNode && !masterNode.equals(activeMasterNode)) {
                    //刷新rpcclient
                    initRpcClient(masterNode);
                }
            }
        }


        return rpcClient;
    }


    private void initRpcClient(WorkerNode masterNode) {
        rpcClient = RpcClient.create(masterNode.getIp(), masterNode.getPort());
        activeMasterNode = masterNode;
    }


    public void heartbeat(RpcHeartbeat rpcHeartbeat) throws Exception {
        heartbeatTryable(rpcHeartbeat);
    }


    private void heartbeatTryable(RpcHeartbeat rpcHeartbeat) throws Exception {
        int count = 0;
        Exception throwE = null;
        long sleepTime = 5L;
        //5s, 10s, 15
        while (++count <= 3) {
            try {
                rpcClient.heartbeat(rpcHeartbeat);
                return;
            } catch (Exception e) {
                throwE = e;
            }

            try {
                TimeUnit.SECONDS.sleep(sleepTime * count);
            } catch (InterruptedException e) {
            }

            getRpcClient();
        }

        throw throwE;
    }


    public void updateTask(RpcTask rpcTask) throws Exception {
        updateTaskTryable(rpcTask);
    }


    private void updateTaskTryable(RpcTask rpcTask) throws Exception {
        int count = 0;
        Exception throwE = null;
        long sleepTime = 5L;
        //5s, 10s, 15s
        while (++count <= 3) {
            try {
                rpcClient.updateTask(rpcTask);
                return;
            } catch (Exception e) {
                throwE = e;
            }

            try {
                TimeUnit.SECONDS.sleep(sleepTime * count);
            } catch (InterruptedException e) {
            }

            getRpcClient();
        }

        throw throwE;
    }
}
