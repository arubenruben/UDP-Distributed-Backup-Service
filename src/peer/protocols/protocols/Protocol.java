package peer.protocols.protocols;

import peer.Peer;
import peer.filesystem.ChunkInfo;
import peer.filesystem.FileInfo;
import peer.protocols.messages.Header;
import peer.protocols.messages.Message;
import peer.protocols.protocols.protocol1_0.RemoveCandidate;
import peer.utils.Constants;
import peer.utils.Logger;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class that defines the basis of what a protocol should do
 */
public abstract class Protocol {
    protected final String mcIp;
    protected final int mcPort;
    protected final String mdbIp;
    protected final int mdbPort;
    protected final Peer peer;
    protected final String version;
    protected final Logger log;

    /**
     * @param mcIp    Control Channel IP
     * @param mcPort  Control Channel Port
     * @param mdbIp   DataBackup IP
     * @param mdbPort DataBackup Port
     * @param peer    Peer reference
     * @param version Protocol version
     */
    public Protocol(String mcIp, int mcPort, String mdbIp, int mdbPort, Peer peer, String version) {
        this.mcIp = mcIp;
        this.mcPort = mcPort;
        this.mdbIp = mdbIp;
        this.mdbPort = mdbPort;
        this.peer = peer;
        this.version = version;
        this.log = new Logger(peer.getId());
    }

    // Default implementations
    //sendHeartbeat and recievedHeartbeat don't have implementation in Protocol 1.0,
    //but are overriden and implemented in Protocol 1.1

    public void sendHeartbeat() {
    }

    public void receivedHeartbeat(Message message) {
    }


    //Interface for Request

    /**
     * backups a file with a specific filename with a desired replication level. If he doesnt achieve that he simples deletes.
     *
     * @param filename
     * @param replicationLevel
     */
    public abstract void backup(String filename, int replicationLevel);

    /**
     * Restore the file with filename provided
     *
     * @param filename
     */
    public abstract void restore(String filename);

    /**
     * Deletes the file with a fileId provided
     *
     * @param fileId
     */
    public abstract void delete(String fileId);

    /**
     * Specify the maximum disk size to the value of the parameter
     *
     * @param availableDiskSpace
     */
    public void reclaim(int availableDiskSpace) {
        log.info("Request for reclaim with size " + availableDiskSpace);
        peer.getFileSystem().setCapacity(availableDiskSpace);

        log.info("Capacity set to " + peer.getFileSystem().getCapacity());

        if (peer.getFileSystem().getExternalFiles().isEmpty()) {
            log.info("No external files - nothing to delete");
            return;
        }

        if (availableDiskSpace == 0) {
            log.info("Notifying peers about the space reclaiming..");
            for (String fileId : peer.getFileSystem().getExternalFiles().keySet()) {
                for (Integer chunkNum : peer.getFileSystem().getExternalFiles().get(fileId).keySet()) {
                    sendRemovedMessage(fileId, chunkNum);
                    log.info("Sent msg REMOVED for file " + fileId + "--> " + chunkNum);

                    try {
                        peer.getFileSystem().deleteChunks(fileId);
                    } catch (IOException e) {
                        log.error("Error deleting chunks of file " + fileId);
                    }
                }
            }
        }

        log.info("Partial space reclaiming");
        PriorityQueue<RemoveCandidate> candidates = createRemoveCandidates();

        log.info("Currently have " + peer.getFileSystem().getOccupiedSpace() + " occupied space out of MAX " + peer.getFileSystem().getCapacity());
        while (peer.getFileSystem().getOccupiedSpace() - peer.getFileSystem().getCapacity() > 0) {
            log.info("Selecting the best chunk to remove..");
            RemoveCandidate best = candidates.remove();

            log.info("Notifying the peers about the chunk removal");

            sendRemovedMessage(best.getFileId(), best.getChunkNumber());
            log.info("Sent msg REMOVED for file " + best.getFileId() + "--> " + best.getChunkNumber());

            try {
                log.info("Removing the chunk " + best.getChunkNumber() + " from file " + best.getFileId());
                peer.getFileSystem().deleteChunk(best.getFileId(), best.getChunkNumber());
                peer.getFileSystem().deleteEmptyFolders();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * @return The Current State of the Peer
     */
    public String state() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("--------------------------------------------------------\n");
        if (peer.getFileSystem().getFilenameFileInfo().isEmpty()) {
            stringBuilder.append("No Files Stored\n");
        } else {
            stringBuilder.append("Files Stored:\n");

            for (FileInfo fileInfo : peer.getFileSystem().getFilenameFileInfo().values()) {
                stringBuilder.append("--------------------------------\n");
                stringBuilder.append("Filename: ").append(fileInfo.getFileName()).append("\n");
                stringBuilder.append("FileId: ").append(fileInfo.getFileId()).append("\n");
                stringBuilder.append("  Desired replication Degree:").append(fileInfo.getDesiredReplicationLevel()).append("\n");

                ConcurrentHashMap<Integer, ChunkInfo> chunkNumChunkInfo = peer.getFileSystem().getInternalFiles().get(fileInfo.getFileId());

                stringBuilder.append("  Chunks of the file:\n");

                for (Integer key : chunkNumChunkInfo.keySet()) {
                    stringBuilder.append("  ------------------------------\n");
                    Integer perceivedReplicationLevel = chunkNumChunkInfo.get(key).getPerceivedReplicationLevel();
                    stringBuilder.append("  Chunk Id: ").append(key).append("\n");
                    stringBuilder.append("  Perceived Replication Degree: ").append(perceivedReplicationLevel).append("\n");
                    stringBuilder.append("  ------------------------------\n");
                }
            }
        }

        if (peer.getFileSystem().getExternalFiles().isEmpty())
            stringBuilder.append("\nNo Chunks Stored\n");
        else {
            stringBuilder.append("\nChunks Stored\n");

            for (String key : peer.getFileSystem().getExternalFiles().keySet()) {
                ConcurrentHashMap<Integer, ChunkInfo> chunkHashMap = peer.getFileSystem().getExternalFiles().get(key);
                stringBuilder.append("--------------------------------\n");
                stringBuilder.append("File Id: ").append(key).append("\n");
                for (Integer chunkNo : chunkHashMap.keySet()) {
                    stringBuilder.append("  Id: ").append(chunkNo).append("\n");
                    stringBuilder.append("  Size: ").append(chunkHashMap.get(chunkNo).getSize()).append("\n");
                    stringBuilder.append("  Desired Replication Degree: ").append(chunkHashMap.get(chunkNo).getDesiredReplicationLevel()).append("\n");
                    stringBuilder.append("  Perceived Replication Degree: ").append(chunkHashMap.get(chunkNo).getPerceivedReplicationLevel()).append("\n\n");
                }
            }
        }
        stringBuilder.append("--------------------------------\n");
        stringBuilder.append("\n").append("Free Space: ").append(peer.getFileSystem().getCapacity() - peer.getFileSystem().getOccupiedSpace()).append("\n");
        stringBuilder.append("Storage Capacity: ").append(peer.getFileSystem().getCapacity());

        return stringBuilder.toString();
    }

    private PriorityQueue<RemoveCandidate> createRemoveCandidates() {
        PriorityQueue<RemoveCandidate> candidates = new PriorityQueue<>();

        for (String fileId : peer.getFileSystem().getExternalFiles().keySet()) {
            for (Integer chunkNum : peer.getFileSystem().getExternalFiles().get(fileId).keySet()) {
                ChunkInfo chunkInfo = peer.getFileSystem().getExternalFiles().get(fileId).get(chunkNum);
                int diff = chunkInfo.getPerceivedReplicationLevel() - chunkInfo.getDesiredReplicationLevel();
                candidates.add(new RemoveCandidate(fileId, chunkNum, diff));
            }
        }

        return candidates;
    }

    public Peer getPeer() {
        return peer;
    }

    public String getVersion() {
        return version;
    }

    /**
     * Send the message for the specified IP and Port in a multicast UDP way.
     *
     * @param message
     * @param ipAddress
     * @param port
     * @throws IOException
     */
    public void sendDatagram(Message message, String ipAddress, int port) throws IOException {
        byte[] byteArray = message.toByteArray();
        MulticastSocket multicastSocket = new MulticastSocket(port);
        DatagramPacket datagramPacket = new DatagramPacket(byteArray, byteArray.length, InetAddress.getByName(ipAddress), port);
        multicastSocket.send(datagramPacket);
    }

    /**
     * The following methods Process a RECEIVED/SEND the message type specified in their header
     * @param request Request received.
     */
    //Received
    public abstract void receivedPutChunk(Message request);

    public abstract void receivedGetChunk(Message request);

    public abstract void receivedDelete(Message request);

    public abstract void receivedStored(Message request);

    public Optional<Message> receivedRemoved(Message notification) {
        if (peer.getId() == notification.getHeader().getSenderId()) {
            return Optional.empty();
        }

        String fileId = notification.getHeader().getFileId();
        int chunkNum = notification.getHeader().getChunkNo();

        // Update environment
        if (peer.getFileSystem().getEnvironmentFilesRepLevel().containsKey(fileId)) {
            if (peer.getFileSystem().getEnvironmentFilesRepLevel().get(fileId).containsKey(chunkNum)) {
                peer.getFileSystem().getEnvironmentFilesRepLevel().get(fileId).get(chunkNum).decrementReplicationLevel();
                try {
                    peer.getFileSystem().writeEnvironmentFilesRepLevelToDisk();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        ConcurrentHashMap<Integer, ChunkInfo> externalChunkNumChunkInfo = peer.getFileSystem().getExternalFiles().get(fileId);
        // If I'm keeping track of that file and also keeping track of that chunk of that file, then update replication level
        if (externalChunkNumChunkInfo != null && externalChunkNumChunkInfo.get(chunkNum) != null) {
            log.info("Updating chunk of a file replication level..");
            int updatedLevel = updateReplicationLevel(fileId, chunkNum);

            int desiredReplicationLevel = externalChunkNumChunkInfo.get(chunkNum).getDesiredReplicationLevel();
            log.info("Current: " + updatedLevel + ", Desired: " + desiredReplicationLevel);

            if (updatedLevel < desiredReplicationLevel) {
                log.info("Chunk " + chunkNum + "'s replication level dropped below desired..");
                try {
                    byte[] buffer = peer.getFileSystem().readChunk(fileId, chunkNum);
                    Message message = new Message(
                            new Header(getVersion(), peer.getId(), fileId, chunkNum, desiredReplicationLevel, "PUTCHUNK"),
                            buffer
                    );
                    log.info("Sending PUTCHUNK for chunk " + chunkNum);
                    peer.getThreadPool().schedule(() -> sendPutChunkHelperReclaim(message, 1), 0, TimeUnit.SECONDS);
                } catch (IOException e) {
                    e.printStackTrace();
                    log.error("Error Opening File");
                }
            }
            try {
                peer.getFileSystem().writeInternalFileMetadataToDisk();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    public abstract void receivedChunk(Message message);

    //Issued
    public void sendStored(Message request) {
        Message reply = new Message(new Header(request.getHeader().getProtocolVersion(), peer.getId(), request.getHeader().getFileId(), request.getHeader().getChunkNo(), "STORED"));

        Random random = new Random();

        peer.getThreadPool().schedule(() -> {
            try {
                sendDatagram(reply, mcIp, mcPort);
            } catch (IOException e) {
                log.error("Unable to reply STORED message");
            }
        }, random.nextInt(Constants.PUTCHUNK_MAX_TIMEOUT), TimeUnit.MILLISECONDS);
    }

    public abstract void sendChunk(Message request);

    public void sendRemovedMessage(String fileId, Integer chunkNum) {
        Message message = new Message(new Header(version, peer.getId(), fileId, chunkNum, "REMOVED"));

        try {
            byte[] byteArray = message.toByteArray();
            MulticastSocket multicastSocket = new MulticastSocket(mcPort);
            DatagramPacket datagramPacket = new DatagramPacket(byteArray, byteArray.length, InetAddress.getByName(mcIp), mcPort);
            multicastSocket.send(datagramPacket);
        } catch (IOException e) {
            log.error("Unable to send the REMOVED message for reclaim");
        }
    }

    /**
     * This method sends a putchunk request until N times or until the replication level of all chunks is achieved. Runs concurrently in a Scheduled Thread for a random time
     *
     * @param request Request to send
     * @param attempt Attempt Number of the request send
     */
    //Helpers
    protected void sendPutChunkHelper(Message request, int attempt) {

        Integer perceivedReplicationDegree = peer.getFileSystem().getInternalFiles().get(request.getHeader().getFileId()).get(request.getHeader().getChunkNo()).getPerceivedReplicationLevel();

        if (perceivedReplicationDegree >= request.getHeader().getDesiredReplicationLevel()) {
            return;
        }

        if (attempt > Constants.MAX_PUTCHUNK_ATTEMPTS) {
            log.error("Backup request timeout for chunk:" + request.getHeader().getChunkNo() + " - Got Replication:" + perceivedReplicationDegree);

            // Delete chunks on the peers who managed to put them
            log.info("Sending DELETE request to the peers for unsuccessful backed up file");
            delete(request.getHeader().getFileId());

            log.info("Cleanup metadata..");
            for (Map.Entry<String, FileInfo> entry : peer.getFileSystem().getFilenameFileInfo().entrySet()) {
                if (entry.getValue().getFileId().equals(request.getHeader().getFileId())) {
                    peer.getFileSystem().getFilenameFileInfo().remove(entry.getKey());
                }
            }

            peer.getFileSystem().getInternalFiles().remove(request.getHeader().getFileId());

            try {
                peer.getFileSystem().writeInternalFileMetadataToDisk();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return;
        }

        try {
            sendDatagram(request, mdbIp, mdbPort);
        } catch (IOException e) {
            log.error("Opening socket but attempt of replication continues");
        }

        peer.getThreadPool().schedule(() -> sendPutChunkHelper(request, attempt + 1), (long) (Math.pow(2, attempt) * Constants.STORED_START_LISTENING_TIMEOUT), TimeUnit.SECONDS);
    }

    protected void sendPutChunkHelperReclaim(Message request, int attempt) {
        Integer perceivedReplicationDegree = peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId()).get(request.getHeader().getChunkNo()).getPerceivedReplicationLevel();

        if (perceivedReplicationDegree >= request.getHeader().getDesiredReplicationLevel()) {
            return;
        }

        if (attempt > Constants.MAX_PUTCHUNK_ATTEMPTS) {
            log.error("Backup request timeout for chunk:" + request.getHeader().getChunkNo() + " - Got Replication:" + perceivedReplicationDegree);

            // Delete chunks on the peers who managed to put them
            log.info("Sending DELETE request to the peers for unsuccessful backed up file");
            delete(request.getHeader().getFileId());

            log.info("Cleanup metadata..");
            for (Map.Entry<String, FileInfo> entry : peer.getFileSystem().getFilenameFileInfo().entrySet()) {
                if (entry.getValue().getFileId().equals(request.getHeader().getFileId())) {
                    peer.getFileSystem().getFilenameFileInfo().remove(entry.getKey());
                }
            }

            peer.getFileSystem().getInternalFiles().remove(request.getHeader().getFileId());

            try {
                peer.getFileSystem().writeInternalFileMetadataToDisk();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return;
        }

        try {
            sendDatagram(request, mdbIp, mdbPort);
        } catch (IOException e) {
            log.error("Opening socket but attempt of replication continues");
        }

        peer.getThreadPool().schedule(() -> sendPutChunkHelperReclaim(request, attempt + 1), (long) (Math.pow(2, attempt) * Constants.STORED_START_LISTENING_TIMEOUT), TimeUnit.SECONDS);
    }

    public abstract void receivedDeleteAck(Message request);

    private int updateReplicationLevel(String fileId, Integer chunkNum) {
        ConcurrentHashMap<Integer, ChunkInfo> chunkNumChunkInfo = peer.getFileSystem().getExternalFiles().get(fileId);
        ChunkInfo chunkInfo = chunkNumChunkInfo.get(chunkNum);
        chunkInfo.decrementReplicationLevel();
        chunkNumChunkInfo.put(chunkNum, chunkInfo);
        return chunkInfo.getPerceivedReplicationLevel();
    }

    private File getFromInternal(String filename) {
        for (File file : peer.getFileSystem().getInternalFilesDir().listFiles()) {
            if (file.getName().equals(filename)) {
                return file;
            }
        }
        return null;
    }
}
