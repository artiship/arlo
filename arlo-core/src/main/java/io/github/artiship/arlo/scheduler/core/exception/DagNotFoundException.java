package io.github.artiship.arlo.scheduler.core.exception;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;

public class DagNotFoundException extends ArloRuntimeException {

    public DagNotFoundException(Long dagId) {
        super("Scheduler dag " + dagId + " not found");
    }
}
