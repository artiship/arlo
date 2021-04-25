package io.github.artiship.arlo.scheduler.manager;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class JobStateStoreTest extends SchedulerDaoTest {

    @Autowired
    private JobStateStore jobStateStore;

    @Test
    public void restore_job_states() {
        assertThat(jobStateStore.getDependencies(job_3.getId())).contains(job_1.getId(), job_2.getId());
        assertThat(jobStateStore.getDependencies(job_4.getId())).contains(job_3.getId());

        assertThat(jobStateStore.hasNoDependencies(job_1.getId())).isTrue();
        assertThat(jobStateStore.hasNoDependencies(job_2.getId())).isTrue();

        assertThat(jobStateStore.hasNoDependencies(job_3.getId())).isFalse();
    }
}