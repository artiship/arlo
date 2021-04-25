package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.AbstractTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class AlertServiceTest extends AbstractTest {
    @Autowired
    private AlertService alertService;

    @Test
    public void test_send_email() {
        alertService.workerDown("xxxxx");
    }

}