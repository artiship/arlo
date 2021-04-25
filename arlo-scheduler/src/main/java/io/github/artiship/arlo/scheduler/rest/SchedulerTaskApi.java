package io.github.artiship.arlo.scheduler.rest;

import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/scheduler/tasks")
public class SchedulerTaskApi {

    @Autowired
    private SchedulerService schedulerService;

    @Autowired
    private TaskScheduler taskScheduler;

    @Autowired
    private TaskDispatcher taskDispatcher;

    @PutMapping("/{taskId}/rerun")
    public ResponseEntity<Long> rerunTask(@PathVariable Long taskId) {
        return ok(schedulerService.rerunTask(taskId));
    }

    @PutMapping("/{taskId}/mark-success")
    public ResponseEntity<Boolean> markSuccessTask(@PathVariable Long taskId) {
        return ok(schedulerService.markSuccessTask(taskId));
    }

    @PutMapping("/{taskId}/mark-fail")
    public ResponseEntity<Boolean> markFailTask(@PathVariable Long taskId) {
        return ok(schedulerService.markFailTask(taskId));
    }

    @PutMapping("/{taskId}/free")
    public ResponseEntity<Void> freeTaskDependencies(@PathVariable Long taskId) {
        schedulerService.freeTask(taskId);
        return ok().build();
    }

    @PutMapping("/{taskId}/kill")
    public ResponseEntity<Boolean> killTask(@PathVariable Long taskId) {
        return ok(schedulerService.killTask(taskId));
    }

    @GetMapping("/pendings")
    public ResponseEntity<Long> pendingCount() {
        return ok(taskScheduler.queuedTaskCount());
    }

    @GetMapping("/waitings")
    public ResponseEntity<Long> waitingCount() {
        return ok(taskDispatcher.queuedTaskCount());
    }
}
