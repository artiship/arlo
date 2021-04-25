package io.github.artiship.arlo.scheduler.worker.util;

import io.github.artiship.arlo.constants.GlobalConstants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class ZkClientHolder implements InitializingBean, DisposableBean {

    @Getter
    private ZkClient zkClient;

    //连接信息
    @Value("${arlo.zookeeper.quorum}")
    private String quorum;

    //session超时
    @Value("${arlo.zookeeper.sessionTimeout}")
    private int sessionTimeout;

    //连接超时
    @Value("${arlo.zookeeper.connectionTimeout}")
    private int connectionTimeout;

    @Value("${arlo.worker.server.port}")
    private int workerPort;

    @Autowired
    private OSSClientHolder ossClientHolder;

    @Override
    public void afterPropertiesSet() throws Exception {
        log.debug("init zk client start.");
        zkClient = new ZkClient(quorum, sessionTimeout, connectionTimeout);
        zkClient.setZkSerializer(new ArloZkSerializer());

        zkClient.subscribeStateChanges(new IZkStateListener() {

            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
                //连上ZK
                if (Watcher.Event.KeeperState.SyncConnected == keeperState) {
                    log.info("connected to zookeeper....");
                    if (zkClient.exists(GlobalConstants.WORKER_GROUP)) {
                        //重新注册自己
                        ZkUtils.regist(workerPort, zkClient, ossClientHolder, false);
                        //watch公共脚本修改目录
                        ZkUtils.watchCommonShellChange(zkClient);
                    }
                }
            }

            @Override
            public void handleNewSession() throws Exception {

            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

            }
        });


        log.debug("init zk client success.");
    }


    @Override
    public void destroy() throws Exception {
        log.debug("stop zk client.");
        if (null != zkClient) {
            try {
                zkClient.close();
            } catch (Exception e) {
            }
        }
    }
}
