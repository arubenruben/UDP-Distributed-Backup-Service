package peer.filesystem;

import java.io.Serializable;

/**
 * Class That stores the information useful about a specific file backed up
 */
public class FileInfo implements Serializable {
    private final String fileName;
    private final String fileId;
    private final int numberOfChunks;
    private final int desiredReplicationLevel;

    public FileInfo(String fileName, String fileId, int numberOfChunks, int replicationLevel) {
        this.fileName = fileName;
        this.numberOfChunks = numberOfChunks;
        this.fileId = fileId;
        this.desiredReplicationLevel = replicationLevel;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFileId() {
        return fileId;
    }

    public int getNumberOfChunks() {
        return numberOfChunks;
    }

    public int getDesiredReplicationLevel() {
        return desiredReplicationLevel;
    }
}
