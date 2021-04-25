package io.github.artiship.arlo.model.exception;

public class ArloParseException extends ArloRuntimeException {

    public ArloParseException() {
        super("解析失败");
    }

    public ArloParseException(String message) {
        super(message);
    }

}
