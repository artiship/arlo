package io.github.artiship.arlo.scheduler.executor.waterdrop;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.executor.AbstractExecutor;
import io.github.artiship.arlo.scheduler.worker.executor.waterdrop.WaterdropExecutor;
import io.github.artiship.arlo.scheduler.worker.updownload.impl.OssUpDownload;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.List;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:applicationContext.xml"})
public class WaterdropExecutorTest {

    @Autowired
    private RpcClientHolder rpcClientHolder;

    @Autowired
    private OSSClientHolder ossClientHolder;

    @Autowired
    private ZkClientHolder zkClientHolder;

    @Test
    public void killLinuxPid() {
        WaterdropExecutor executor = new WaterdropExecutor(null, null, null, null);
        try {
            Method method = AbstractExecutor.class.getDeclaredMethod("killLinuxProcessByPid", Integer.class);
            method.setAccessible(true);
            method.invoke(executor, 9060);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }

        System.out.println("over.");
    }

    @Test
    public void getChildPids() {
        WaterdropExecutor executor = new WaterdropExecutor(null, null, null, null);
        try {
            Method method = AbstractExecutor.class.getDeclaredMethod("getPidTree", Integer.class);
            method.setAccessible(true);
            List<String> childPids = (List<String>) method.invoke(executor, 9060);
            childPids.forEach((childPid) -> System.out.println(childPid));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void execute() {
        SchedulerTaskBo task = new SchedulerTaskBo();
        task.setOssPath("arlo/arlo_test_001/version_126");
        task.setCalculationTime(LocalDateTime.now());
        task.setId(126L);

        WaterdropExecutor waterdropExecutor = new WaterdropExecutor(task, rpcClientHolder, ossClientHolder, zkClientHolder);
        waterdropExecutor.execute();
    }

    @Test
    public void generatorShell() {
        String normalLogFile = "arlo/arlo_test_001/version_126/start-all.sh";

        OssUpDownload ossUpdownload = new OssUpDownload(normalLogFile, ossClientHolder);

        //start-all.sh
        ossUpdownload.uploadLog("echo \"111\"\n");
        ossUpdownload.uploadLog("nohup sh start-child.sh > start-child.log 2>&1 &\n");
        ossUpdownload.uploadLog("sleep 20\n");
        ossUpdownload.uploadLog("echo \"222\"\n");
    }
}
