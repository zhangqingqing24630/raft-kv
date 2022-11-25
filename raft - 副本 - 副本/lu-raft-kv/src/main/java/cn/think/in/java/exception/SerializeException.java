package cn.think.in.java.exception;

public class SerializeException extends RuntimeException {
    public SerializeException() {
        super();
    }

    public SerializeException(String message) {
        super(message);
    }

    public SerializeException(Throwable cause) {
        super(cause);
    }

    public SerializeException(String message, Throwable cause) {
        super(message, cause);
    }
}
