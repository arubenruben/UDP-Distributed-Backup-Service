package peer.filesystem;

/**
 * Class with information about the state of other peers in the environment. Used in the 1.1 as consequence of the heart beat message
 */
public class PeerStatus {
    private int freeSpace;
    private long lastUpdatedAt;

    public PeerStatus(int freeSpace, long lastUpdatedAt) {
        this.freeSpace = freeSpace;
        this.lastUpdatedAt = lastUpdatedAt;
    }

    public int getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(int freeSpace) {
        this.freeSpace = freeSpace;
    }

    public long getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public void setLastUpdatedAt(long lastUpdatedAt) {
        this.lastUpdatedAt = lastUpdatedAt;
    }
}
