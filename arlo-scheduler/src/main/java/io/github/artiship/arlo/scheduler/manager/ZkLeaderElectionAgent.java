package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.model.ZkMaster;
import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;
import static io.github.artiship.arlo.constants.GlobalConstants.MASTER_GROUP;
import static io.github.artiship.arlo.model.enums.NodeState.ACTIVE;
import static io.github.artiship.arlo.model.enums.NodeState.STANDBY;
import static io.github.artiship.arlo.model.enums.NodeType.MASTER;
import static io.github.artiship.arlo.utils.CuratorUtils.createPath;
import static io.github.artiship.arlo.utils.MetricsUtils.getHostIpAddress;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STOPPED;

@Slf4j
@Component
public class ZkLeaderElectionAgent implements LeaderLatchListener, Service, ConnectionStateListener {
    @Resource
    private CuratorFramework zkClient;
    @Autowired
    private SchedulerDao schedulerDao;

    @Value("${server.port}")
    private int http_port;
    @Value("${rpc.port}")
    private int rpc_port = 9090;

    private LeaderLatch leaderLatch;
    private LeadershipStatus status = LeadershipStatus.NOT_LEADER;

    private LeaderElectable master;

    public void register(LeaderElectable master) {
        this.master = master;
    }

    @Override
    public void start() throws Exception {
        zkClient.getZookeeperClient()
                .blockUntilConnectedOrTimedOut();
        zkClient.getConnectionStateListenable()
                .addListener(this);

        leaderLatch = new LeaderLatch(zkClient, createPath(zkClient, MASTER_GROUP));
        leaderLatch.addListener(this);
        leaderLatch.start();
        leaderLatch.await();
    }

    @Override
    public void stop() throws IOException {
        if (zkClient.getState() == STOPPED)
            return;

        if (leaderLatch.getState() == LeaderLatch.State.STARTED) {
            leaderLatch.close();
        }

        if (zkClient.getState() == STARTED) {
            zkClient.close();
        }
    }

    @Override
    public void isLeader() {
        synchronized (this) {
            // could have lost leadership by now.
            if (!leaderLatch.hasLeadership()) {
                return;
            }

            try {
                ZkMaster zkMaster = new ZkMaster(getHostIpAddress(), rpc_port, http_port);
                zkClient.setData()
                        .forPath(MASTER_GROUP, zkMaster.toString()
                                                       .getBytes(UTF_8));
            } catch (Exception e) {
                log.error("Set data failed", e);
            }

            log.info("We have gained leadership");
            updateLeadershipStatus(true);
        }
    }

    @Override
    public void notLeader() {
        synchronized (this) {
            // could have gained leadership by now.
            if (leaderLatch.hasLeadership()) {
                return;
            }

            log.info("We have lost leadership");
            updateLeadershipStatus(false);
        }
    }

    private void updateLeadershipStatus(boolean isLeader) {
        if (isLeader && status == LeadershipStatus.NOT_LEADER) {
            status = LeadershipStatus.LEADER;
            master.electedLeader();
        } else if (!isLeader && status == LeadershipStatus.LEADER) {
            status = LeadershipStatus.NOT_LEADER;
            master.revokedLeadership();
        }
    }

    @Override
    public void stateChanged(CuratorFramework zkClient, ConnectionState state) {
        switch (state) {
            case SUSPENDED:
            case RECONNECTED:
            case LOST:
            case READ_ONLY:
            case CONNECTED:
        }

        log.info("Zookeeper state change to {}", state);
    }

    private void updateDb() {
        try {
            schedulerDao.saveNode(new SchedulerNodeBo()
                    .setHost(getHostIpAddress())
                    .setPort(this.rpc_port)
                    .setNodeType(MASTER)
                    .setNodeState(status == LeadershipStatus.LEADER ? ACTIVE : STANDBY));
        } catch (Exception e) {
            log.warn("Save master info to db fail.", e);
        }
    }

    private enum LeadershipStatus {
        LEADER, NOT_LEADER;
    }
}
