package peer.protocols.protocols.protocol1_1;

import peer.Peer;
import peer.protocols.messages.Header;
import peer.protocols.messages.Message;
import peer.utils.Constants;
import peer.utils.Logger;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates all the login of incoming requests of protocol 1.1
 */
public class Protocol1_1InternalSend {
    private final Protocol1_1 protocol;
    private final Peer peer;
    private final String mcIp;
    private final int mcPort;
    private final String mdbIp;
    private final int mdbPort;
    private final String mdrIp;
    private final int mdrPort;
    private final Logger log;

    /**
     * @param protocol
     * @param peer
     * @param mcIp
     * @param mcPort
     * @param mdbIp
     * @param mdbPort
     * @param mdrIp
     * @param mdrPort
     * @param log
     */
    public Protocol1_1InternalSend(Protocol1_1 protocol, Peer peer, String mcIp, int mcPort, String mdbIp, int mdbPort, String mdrIp, int mdrPort, Logger log) {
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

    /**
     * This method is executed a random time after receiving the GETCHUNK requests. It aborts if the peer as the sure some other peer has replied the same message
     *
     * @param reply
     * @throws IOException
     */
    private void sendChunkHelper(Message reply) throws IOException {

        CopyOnWriteArrayList<Integer> chunkListenedList = peer.getFileSystem().getChunksListened().get(reply.getHeader().getFileId());

        if (chunkListenedList.contains(reply.getHeader().getChunkNo()))
            return;

        protocol.sendDatagram(reply, mdrIp, mdrPort);

        Socket socket;
        byte[] buffer;

        try {
            socket = new Socket(Constants.TCP_HOST_NAME, reply.getHeader().getTcpPort());
        } catch (IOException e) {
            log.error("Error opening the socket to reply with chunk");
            return;
        }

        try {
            buffer = peer.getFileSystem().readChunk(reply.getHeader().getFileId(), reply.getHeader().getChunkNo());
        } catch (IOException e) {
            log.error("Error sending the chunk");
            return;
        }
        try {
            log.info("Sending via TCP socket chunk " + reply.getHeader().getChunkNo());
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            Message tcpMessage = new Message(reply);
            tcpMessage.setBody(buffer);
            outputStream.writeObject(tcpMessage);
            socket.close();
        } catch (IOException e) {
            log.error("Error opening an output stream in the TCP socket to reply the chunk");
            return;
        }
    }

    public void sendHeartbeat() {
        int myFreeSpace = peer.getFileSystem().getCapacity() - peer.getFileSystem().getOccupiedSpace();

        log.info("Sending a heartbeat - free space: " + myFreeSpace + " bytes");
        Message message = new Message(new Header("1.1", protocol.getPeer().getId(), "HEARTBEAT"),
                ByteBuffer.allocate(4).putInt(myFreeSpace).array());

        Random random = new Random();

        peer.getThreadPool().schedule(() -> {
            try {
                protocol.sendDatagram(message, mcIp, mcPort);
            } catch (IOException e) {
                log.error("Unable to send HEARTBEAT message");
            }
        }, random.nextInt(Constants.PUTCHUNK_MAX_TIMEOUT), TimeUnit.MILLISECONDS);

    }
}
