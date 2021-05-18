package peer.filesystem;

import peer.protocols.messages.Message;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class encapsulates the state of a restore file request
 */
public class FileRestorer implements Serializable {

    private final ConcurrentHashMap<Integer, byte[]> chunks;

    public FileRestorer() {
        this.chunks = new ConcurrentHashMap<>();
    }

    public void collectChunk(Message message) {

        if (chunks.get(message.getHeader().getChunkNo()) != null)
            return;

        chunks.put(message.getHeader().getChunkNo(), message.getBody());
    }

    public int getNumberOfChunksStored() {
        return chunks.size();
    }

    public ConcurrentHashMap<Integer, byte[]> getChunks() {
        return chunks;
    }
}
