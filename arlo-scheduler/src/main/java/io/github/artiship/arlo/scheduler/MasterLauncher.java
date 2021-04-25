package io.github.artiship.arlo.scheduler;

import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.manager.DagScheduler;
import io.github.artiship.arlo.scheduler.manager.JobStateStore;
import io.github.artiship.arlo.scheduler.manager.LeaderElectable;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.manager.ZkLeaderElectionAgent;
import io.github.artiship.arlo.scheduler.manager.ZkLostTaskListener;
import io.github.artiship.arlo.scheduler.manager.ZkWorkerListener;
import io.github.artiship.arlo.scheduler.rpc.MasterRpcServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.Lists.reverse;
import static java.lang.Runtime.getRuntime;
import static java.util.Arrays.asList;

@Slf4j
@SpringBootApplication(scanBasePackages = {"io.github.artiship.arlo.scheduler", "io.github.artiship.arlo.db"})
public class MasterLauncher implements LeaderElectable {
    private static CountDownLatch isStop = new CountDownLatch(1);

    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private TaskScheduler taskScheduler;
    @Autowired
    private MasterRpcServer masterRpcServer;
    @Autowired
    private QuartzSchedulerService quartzSchedulerService;
    @Autowired
    private DagScheduler dagScheduler;
    @Autowired
    private ZkLeaderElectionAgent zkLeaderElectionAgent;
    @Autowired
    private ZkWorkerListener zkWorkerListener;
    @Autowired
    private ZkLostTaskListener zkLostTaskListener;

    private List<Service> services = new ArrayList<>();

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MasterLauncher.class, args);
        MasterLauncher lau = context.getBean(MasterLauncher.class);
        lau.start();

        try {
            isStop.await();
        } catch (InterruptedException e) {
            log.error("Master is stop latch await was terminated", e);
        }

        System.exit(SpringApplication.exit(context, () -> 0));
    }

    public void start() {
        zkLeaderElectionAgent.register(this);
        asList(zkLeaderElectionAgent,
                masterRpcServer,
                zkLostTaskListener,
                zkWorkerListener,
                jobStateStore,
                taskDispatcher,
                taskScheduler,
                dagScheduler,
                quartzSchedulerService
        ).forEach(service -> {
            services.add(service);
            try {
                service.start();
            } catch (Exception e) {
                log.error("Master start fail", e);
                stop();
            }
        });
        getRuntime().addShutdownHook(new Thread(() -> stop()));
    }

    @Override
    public void electedLeader() {
        log.info("Elected as a master");
    }

    @Override
    public void revokedLeadership() {
        stop();
    }

    public void stop() {
        reverse(services).forEach(service -> {
            try {
                service.stop();
            } catch (Exception e) {
                log.error("Master stop fail", e);
            }
        });

        isStop.countDown();
    }
}
