package peer.filesystem;

import java.io.Serializable;

/**
 * Class that stores all the information regarding the storage of a specific chunk
 */
public class ChunkInfo implements Serializable {
    private int size;
    private final int desiredReplicationLevel;
    private int perceivedReplicationLevel;

    public ChunkInfo(int size, int desiredReplicationLevel, int perceivedReplicationLevel) {
        this.size = size;
        this.desiredReplicationLevel = desiredReplicationLevel;
        this.perceivedReplicationLevel = perceivedReplicationLevel;
    }

    public int getSize() {
        return size;
    }

    public int getDesiredReplicationLevel() {
        return desiredReplicationLevel;
    }

    public int getPerceivedReplicationLevel() {
        return perceivedReplicationLevel;
    }

    public void incrementReplicationLevel() {
        perceivedReplicationLevel++;
    }

    public void decrementReplicationLevel() {
        perceivedReplicationLevel--;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
