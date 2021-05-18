package peer.protocols.protocols.protocol1_0;

/**
 * Class that encapsulates the login behind the choose of what chunks to delete when a reclaim is issued and there is no more space to keep track of it
 */

public class RemoveCandidate implements Comparable<RemoveCandidate> {
    private String fileId;
    private int chunkNumber;
    // diff is chunk's PerceivedReplicationLevel - DesiredReplicationLevel
    private int diff;

    public RemoveCandidate(String fileId, int chunkNumber, int difference) {
        this.fileId = fileId;
        this.chunkNumber = chunkNumber;
        this.diff = difference;
    }

    public String getFileId() {
        return fileId;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }

    @Override
    public int compareTo(RemoveCandidate o) {
        return Integer.valueOf(diff).compareTo(o.diff);
    }
}
