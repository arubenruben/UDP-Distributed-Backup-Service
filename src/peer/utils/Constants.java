package peer.utils;

import java.io.File;

/**
 * Constants used in the project
 */
public class Constants {

    public final static int MAX_MESSAGE_SIZE = 65000;
    public final static int MAX_CHUNK_SIZE = 64000;
    public final static int MAX_CONTROL_MSG_SIZE = 40000;
    public final static int MAX_REPLICATION_DEGREE = 9;
    public final static int RMI_PORT = 1099;
    public final static String CHUNK_BASE_PATH = File.separator + "chunks" + File.separator;
    public final static String FILE_BASE_PATH = File.separator + "files" + File.separator;
    public final static String METADATA_PATH = File.separator + "meta" + File.separator;
    public static final String RESTORE_PATH = File.separator + "restore" + File.separator;
    public final static int MAX_DISK_CAPACITY = 200000;
    public final static String CRLF_STR = "\r\n";
    public final static int PUTCHUNK_MAX_TIMEOUT = 400;
    public final static long STORED_START_LISTENING_TIMEOUT = 1;
    public final static int MAX_PUTCHUNK_ATTEMPTS = 5;
    public final static int CHUNK_MAX_TIMEOUT = 400;
    public final static int ATTEMPTS_TO_DELETE = 5;
    public final static int THREAD_POOL_SIZE = 10;
    public final static int TCP_SERVER_SOCKET_PORT = 5558;
    public final static String TCP_HOST_NAME = "localhost";
}
