package io.github.artiship.arlo.scheduler.executor.updownload;

import io.github.artiship.arlo.scheduler.worker.updownload.impl.OssUpDownload;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:applicationContext.xml"})
public class OssUpdownloadTest {

    @Autowired
    private OSSClientHolder ossClientHolder;

    @Test
    public void testDownload() {
        OssUpDownload ossUpdownload = new OssUpDownload("", ossClientHolder);
        try {
            ossUpdownload.download("arlo/arlo_107/107_573197785051160", "/Users/arlo/package");
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("over...");
    }

    @Test
    public void testUpload() {

        String normalLogFile = "arlo/arlo_test_001/version_123/start-all.sh";

        OssUpDownload ossUpdownload = new OssUpDownload(normalLogFile, ossClientHolder);

        for (int j = 0; j < 10; j++) {
            ossUpdownload.uploadLog("normal-log-" + j);
        }

        System.out.println("over...");
    }

}
