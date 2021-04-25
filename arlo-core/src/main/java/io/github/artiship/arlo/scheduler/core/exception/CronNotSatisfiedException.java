package io.github.artiship.arlo.scheduler.core.exception;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;

public class CronNotSatisfiedException extends ArloRuntimeException {

    public CronNotSatisfiedException(String message) {
        super(message);
    }
}
