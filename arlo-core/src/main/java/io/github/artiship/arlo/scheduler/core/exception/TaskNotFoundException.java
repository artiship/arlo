package io.github.artiship.arlo.scheduler.core.exception;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;

public class TaskNotFoundException extends ArloRuntimeException {

    public TaskNotFoundException(Long taskId) {
        super("Scheduler task " + taskId + " not found");
    }
}
