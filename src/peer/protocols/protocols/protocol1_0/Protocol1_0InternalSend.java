package peer.protocols.protocols.protocol1_0;

import peer.Peer;
import peer.protocols.messages.Message;
import peer.utils.Constants;
import peer.utils.Logger;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates the logic behind all outgoing requests of the protocol 1.0
 */
public class Protocol1_0InternalSend {

    private final Protocol1_0 protocol;
    private final Peer peer;
    private final String mcIp;
    private final int mcPort;
    private final String mdbIp;
    private final int mdbPort;
    private final Logger log;
    private String mdrIp;
    private int mdrPort;

    public Protocol1_0InternalSend(Protocol1_0 protocol, Peer peer, String mcIp, int mcPort, String mdbIp, int mdbPort, String mdrIp, int mdrPort, Logger log) {
        this.protocol = protocol;
        this.peer = peer;
        this.mcIp = mcIp;
        this.mcPort = mcPort;
        this.mdbIp = mdbIp;
        this.mdbPort = mdbPort;
        this.mdrIp = mdrIp;
        this.mdrPort = mdrPort;
        this.log = log;
    }

    public void sendChunk(Message reply) {

        Random random = new Random();

        peer.getThreadPool().schedule(() -> {
            try {
                sendChunkHelper(reply);
            } catch (IOException e) {
                log.error("Error sending CHUNK reply");
            }
        }, random.nextInt(Constants.CHUNK_MAX_TIMEOUT), TimeUnit.MILLISECONDS);
    }

    private void sendChunkHelper(Message reply) throws IOException {

        CopyOnWriteArrayList<Integer> chunkListenedList = peer.getFileSystem().getChunksListened().get(reply.getHeader().getFileId());

        if (chunkListenedList.contains(reply.getHeader().getChunkNo()))
            return;

        protocol.sendDatagram(reply, mdrIp, mdrPort);
    }

}
