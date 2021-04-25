package io.github.artiship.arlo.scheduler.rest;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/scheduler/workers")
public class SchedulerWorkerApi {
    @Autowired
    private SchedulerService schedulerService;

    @PutMapping("/{workerHost}/shutdown")
    public ResponseEntity<Void> shutdownWorker(@PathVariable String workerHost) {
        schedulerService.shutdownWorker(workerHost);
        return ok().build();
    }

    @PutMapping("/{workerHost}/resume")
    public ResponseEntity<Void> resumeWorker(@PathVariable String workerHost) {
        schedulerService.resumeWorker(workerHost);
        return ok().build();
    }

    @GetMapping("")
    public ResponseEntity<List<SchedulerNodeBo>> getWorkers() {
        return ok(schedulerService.getWorkers());
    }
}
