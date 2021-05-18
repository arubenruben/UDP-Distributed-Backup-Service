package peer.utils;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Logging Class to help keep track of the logs made in a prettier way.
 */
public class Logger {
    private int peer;
    private static final String ERROR = "ERROR";
    private static final String WARN = "WARN";
    private static final String INFO = "INFO";

    public Logger(int peerId) {
        peer = peerId;
    }

    public void error(String message) {
        log(ERROR, message);
    }

    public void warn(String message) {
        log(WARN, message);
    }

    public void info(String message) {
        log(INFO, message);
    }

    private void log(String level, String message) {
        Timestamp ts = Timestamp.from(Instant.now());
        System.out.printf("[peer=%d][%s][%s]: %s\n", peer, level, ts.toString(), message);
    }
}
