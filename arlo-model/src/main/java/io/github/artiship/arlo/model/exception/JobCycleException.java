package io.github.artiship.arlo.model.exception;

public class JobCycleException extends ArloRuntimeException {


    public JobCycleException(int errorCode, String message) {
        super(errorCode, message);
    }
}
