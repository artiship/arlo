package io.github.artiship.arlo.model.exception;

public class ArloRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private int errorCode;

    public ArloRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArloRuntimeException(Throwable cause) {
        super(cause);
    }

    public ArloRuntimeException(String message) {
        super(message);
    }

    public ArloRuntimeException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ArloRuntimeException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
}
