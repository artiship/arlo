package io.github.artiship.arlo.scheduler.rest;

import io.github.artiship.arlo.model.vo.ComplementDownStreamRequest;
import io.github.artiship.arlo.scheduler.manager.DagScheduler;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

import static org.springframework.http.ResponseEntity.ok;

@RestController
@RequestMapping("/scheduler/dags")
public class SchedulerDagApi {

    @Autowired
    private SchedulerService schedulerService;

    @Autowired
    private DagScheduler dagScheduler;

    @GetMapping("/running")
    public ResponseEntity<Set<Long>> runningDags() {
        return ok(dagScheduler.getRunningDags());
    }

    @PostMapping("/")
    public ResponseEntity<Void> runDag(@RequestBody ComplementDownStreamRequest request) {
        this.schedulerService.runDag(request);
        return ok().build();
    }

    @PutMapping("/{dagId}/run")
    public ResponseEntity<Void> runDag(@PathVariable Long dagId) {
        this.schedulerService.runDag(dagId);
        return ok().build();
    }

    @PutMapping("/{dagId}/stop")
    public ResponseEntity<Void> stopDag(@PathVariable Long dagId) {
        this.schedulerService.stopDag(dagId);
        return ok().build();
    }
}
