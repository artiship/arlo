package io.github.artiship.arlo.scheduler.rest;

import io.github.artiship.arlo.model.vo.ComplementRequest;
import io.github.artiship.arlo.model.vo.JobRelation;
import io.github.artiship.arlo.scheduler.model.TaskSuccessRecord;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/scheduler/jobs")
public class SchedulerJobApi {

    @Autowired
    private SchedulerService schedulerService;

    @PutMapping("/{jobId}/schedule")
    public ResponseEntity<Void> scheduleJob(@PathVariable Long jobId) {
        schedulerService.scheduleJob(jobId);
        return ok().build();
    }

    @PutMapping("/{jobId}/pause")
    public ResponseEntity<Void> pauseJob(@PathVariable Long jobId) {
        schedulerService.pauseJob(jobId);
        return ok().build();
    }

    @PutMapping("/{jobId}/resume")
    public ResponseEntity<Void> resumeJob(@PathVariable Long jobId) {
        schedulerService.resumeJob(jobId);
        return ok().build();
    }

    @DeleteMapping("/{jobId}/delete")
    public ResponseEntity<Void> deleteJob(@PathVariable Long jobId) {
        schedulerService.deleteJob(jobId);
        return ok().build();
    }

    @PostMapping("/{jobId}/dependencies")
    public ResponseEntity<Void> addDependency(@PathVariable Long jobId,
                                              @RequestBody List<JobRelation> jobRelations) {

        this.schedulerService.addJobDependencies(jobId, jobRelations);
        return ok().build();
    }

    @GetMapping("/{jobId}/dependencies")
    public ResponseEntity<Set<Long>> getDependencies(@PathVariable Long jobId) {
        return ok(this.schedulerService.getJobDependencies(jobId));
    }

    @GetMapping("/{jobId}/runnings")
    public ResponseEntity<Set<Long>> getRunningTasks(@PathVariable Long jobId) {
        return ok(this.schedulerService.getRunningTasks(jobId));
    }

    @GetMapping("/{jobId}/successes")
    public ResponseEntity<Set<TaskSuccessRecord>> getSuccesses(@PathVariable Long jobId) {
        return ok(this.schedulerService.getJobSuccesses(jobId));
    }

    @DeleteMapping("/{jobId}/dependencies/(parentJobId}")
    public ResponseEntity<Void> removeDependency(@PathVariable Long jobId, @PathVariable Long parentJobId) {
        this.schedulerService.removeJobDependency(jobId, parentJobId);
        return ok().build();
    }

    @PostMapping("/{jobId}/dependencies/delete")
    public ResponseEntity<Void> removeDependency(@PathVariable Long jobId,
                                                 @RequestBody List<JobRelation> jobRelations) {
        this.schedulerService.removeJobDependencies(jobId, jobRelations);
        return ok().build();
    }

    @PutMapping("/{jobId}/run")
    public ResponseEntity<Long> runJob(@PathVariable Long jobId) {
        return ok(schedulerService.runJob(jobId));
    }

    @PutMapping("/{jobId}/run/{pointInTime}")
    public ResponseEntity<Long> runJobAtPointInTime(@PathVariable Long jobId,
                                                    @PathVariable(value = "pointInTime")
                                                    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime pointInTime) {
        return ok(schedulerService.runJob(jobId, pointInTime));
    }

    @PutMapping("/{jobId}/run/{startTime}/to/{endTime}")
    public ResponseEntity<Void> runJobInTimeRange(@PathVariable Long jobId,
                                                  @PathVariable(value = "startTime")
                                                  @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime fromTime,
                                                  @PathVariable(value = "endTime")
                                                  @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime toTime) {

        schedulerService.runJob(new ComplementRequest().setStartTime(fromTime)
                                                       .setEndTime(toTime)
                                                       .setCheckDependency(true)
                                                       .setJobId(jobId));
        return ok().build();
    }

    @PostMapping("/{jobId}/run")
    public ResponseEntity<Void> runJobInTimeRange(@PathVariable Long jobId,
                                                  @RequestBody ComplementRequest jobComplementRequest) {
        requireNonNull(jobId, "Job id is null");
        schedulerService.runJob(jobComplementRequest);
        return ok().build();
    }
}
