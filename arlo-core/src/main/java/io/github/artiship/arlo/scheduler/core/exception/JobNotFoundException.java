package io.github.artiship.arlo.scheduler.core.exception;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;

public class JobNotFoundException extends ArloRuntimeException {

    public JobNotFoundException(Long jobId) {
        super("Scheduler job " + jobId + " not found");
    }
}
