package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.db.repository.SchedulerJobRelationRepository;
import io.github.artiship.arlo.db.repository.SchedulerJobRepository;
import io.github.artiship.arlo.db.repository.SchedulerTaskRepository;
import io.github.artiship.arlo.model.entity.SchedulerJob;
import io.github.artiship.arlo.model.entity.SchedulerTask;
import io.github.artiship.arlo.model.vo.JobRelation;
import io.github.artiship.arlo.scheduler.AbstractTest;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.github.artiship.arlo.model.entity.SchedulerJobRelation.of;
import static io.github.artiship.arlo.model.enums.TaskState.SUCCESS;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class SchedulerDaoTest extends AbstractTest {
    @Autowired
    protected SchedulerDao schedulerDao;
    protected SchedulerJob job_1;
    protected SchedulerJob job_2;
    protected SchedulerJob job_3;
    protected SchedulerJob job_4;
    protected SchedulerJobRelation relation_3_1;
    protected SchedulerJobRelation relation_3_2;
    protected SchedulerJobRelation relation_4_3;
    protected SchedulerTask task_1_1;
    protected SchedulerTask task_1_2;
    protected SchedulerTask task_1_3;
    protected SchedulerTask task_2_1;
    protected SchedulerTask task_2_2;
    protected SchedulerTask task_2_3;
    protected SchedulerTask task_3_1;
    protected SchedulerTask task_3_2;
    protected SchedulerTask task_3_3;
    protected SchedulerTask task_4_1;
    protected SchedulerTask task_4_2;
    protected SchedulerTask task_4_3;
    @Autowired
    private SchedulerJobRepository schedulerJobRepository;
    @Autowired
    private SchedulerJobRelationRepository schedulerJobRelationRepository;
    @Autowired
    private SchedulerTaskRepository schedulerTaskRepository;

    @Before
    public void setup() {
        job_1 = schedulerJobRepository.save(mockSchedulerJobInfo("job_1").setIsSelfDependent(true));
        job_2 = schedulerJobRepository.save(mockSchedulerJobInfo("job_2"));
        job_3 = schedulerJobRepository.save(mockSchedulerJobInfo("job_3"));
        job_4 = schedulerJobRepository.save(mockSchedulerJobInfo("job_4"));

        relation_3_1 = schedulerJobRelationRepository.save(of(job_3.getId(), job_1.getId()));
        relation_3_2 = schedulerJobRelationRepository.save(of(job_3.getId(), job_2.getId()));
        relation_4_3 = schedulerJobRelationRepository.save(of(job_4.getId(), job_3.getId()));
    }

    @Test
    public void load_job_and_relations() {
        SchedulerJobBo jobAndDependencies = schedulerDao.getJobAndDependencies(job_3.getId());
        assertThat(jobAndDependencies.getDependencies()).hasSize(2);
    }

    @Test
    public void get_relations() {
        List<JobRelation> jobRelations = schedulerDao.getJobRelations();
        assertThat(jobRelations).isNotEmpty();
    }

    @Test
    public void save_or_update_task() throws InterruptedException {
        SchedulerTask schedulerTask = schedulerTaskRepository.save(mockSchedulerTask(job_1.getId()));
        TimeUnit.SECONDS.sleep(1);
        SchedulerTask schedulerTaskUpdated = schedulerTaskRepository.save(schedulerTask);

        assertThat(schedulerTask.getUpdateTime()).isNotEqualTo(schedulerTaskUpdated.getUpdateTime());
        schedulerTaskRepository.delete(schedulerTask);
    }

    @Test
    public void should_task_save() throws InterruptedException {
        SchedulerTask schedulerTask = this.mockSchedulerTask(job_1.getId())
                                          .setCreateTime(now())
                                          .setUpdateTime(now());
        schedulerTaskRepository.save(schedulerTask);
        schedulerTaskRepository.save(schedulerTask);
        schedulerTaskRepository.delete(schedulerTask);
    }

    @After
    public void teardown() {
        schedulerJobRepository.deleteAll(asList(job_1, job_2, job_3));
        schedulerJobRelationRepository.deleteAll(asList(relation_3_1, relation_3_2, relation_4_3));
    }

    protected SchedulerJob mockSchedulerJobInfo(String jobName) {
        return new SchedulerJob().setJobName(jobName)
                                 .setAlertIds("1,2,3")
                                 .setAlertUsers("1,2,3")
                                 .setJobPriority(1)
                                 .setJobReleaseState(1)
                                 .setScheduleCron("0 1 * * * ?")
                                 .setJobType(1);
    }

    protected SchedulerTask mockSchedulerTask(Long jobId) {
        return new SchedulerTask().setTaskState(SUCCESS.getCode())
                                  .setTaskState(SUCCESS.getCode())
                                  .setJobId(jobId);
    }
}