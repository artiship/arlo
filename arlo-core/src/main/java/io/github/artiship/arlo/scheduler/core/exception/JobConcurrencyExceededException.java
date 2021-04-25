package io.github.artiship.arlo.scheduler.core.exception;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;

public class JobConcurrencyExceededException extends ArloRuntimeException {
    public JobConcurrencyExceededException(String message) {
        super(message);
    }
}
