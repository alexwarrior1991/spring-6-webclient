package guru.springframework.exception;

/**
 * Exception thrown when a client error occurs.
 * Typically used for HTTP 4xx responses.
 */
public class ClientException extends RuntimeException {

    public ClientException() {
        super();
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }
}
