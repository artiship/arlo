package io.github.artiship.arlo.scheduler.core.exception;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;

public class WorkerBusyException extends ArloRuntimeException {
    public WorkerBusyException(String message) {
        super(message);
    }
}
