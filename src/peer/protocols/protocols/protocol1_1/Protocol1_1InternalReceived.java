package peer.protocols.protocols.protocol1_1;

import peer.Peer;
import peer.filesystem.ChunkInfo;
import peer.filesystem.PeerStatus;
import peer.protocols.messages.Header;
import peer.protocols.messages.Message;
import peer.utils.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class encapsulates the logic behind all outgoing requests of the protocol 1.1
 */
public class Protocol1_1InternalReceived {

    private final Protocol1_1 protocol;
    private final Peer peer;
    private final Logger log;

    //This represents how long it is allowed not to send Heartbeat msg
    //before we assume a peer is dead in ms
    //Current = 1 minute = 60 000 ms
    private final static long ALIVE_PEERS_THRESHOLD = 60000;

    //This constant represents the tradeoff between memory and performance
    //The bigger it is, the performance is better and memory is more occupied -> more peers store the chunks
    //The smaller it is, the memory is less occupied and performance is worse -> less peers store the chunks
    private final static int BIAS = 2;

    /**
     * @param protocol Protocol instance
     * @param peer     Peer reference
     * @param log      Logger reference
     */
    public Protocol1_1InternalReceived(Protocol1_1 protocol, Peer peer, Logger log) {
        this.protocol = protocol;
        this.peer = peer;
        this.log = log;
    }

    public void receivedStored(Message request) {
        String fileId = request.getHeader().getFileId();

        updateEnvironmentFilesReplicationLevels(request);

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        ConcurrentHashMap<Integer, ChunkInfo> fileBackupHash = peer.getFileSystem().getInternalFiles().get(request.getHeader().getFileId());

        //Keeping track of that file
        if (fileBackupHash != null) {
            processStoreRequest(true, request, fileBackupHash);
            if (peer.getFileSystem().getInternalFiles().containsKey(fileId)){
                if (chunkBackedUp(request.getHeader().getFileId(),request.getHeader().getChunkNo())){
                    log.info("Backup completed for chunk: " + request.getHeader().getChunkNo() + " of file " + peer.getFileSystem().idToFilename(fileId));
                    if (allChunksStored(fileId)){
                        log.info("File successfully backed up: " + peer.getFileSystem().idToFilename(fileId));
                    }
                }
            }
            return;
        }

        //Keep record of the perceptions on the environment
        ConcurrentHashMap<Integer, ChunkInfo> storedHash = peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId());

        if (storedHash != null) {
            processStoreRequest(false, request, storedHash);
            return;
        }
    }

    private void updateEnvironmentFilesReplicationLevels(Message request) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> environmentFilesRepLevel = peer.getFileSystem().getEnvironmentFilesRepLevel();
        if (!environmentFilesRepLevel.containsKey(request.getHeader().getFileId())) {
            environmentFilesRepLevel.put(request.getHeader().getFileId(), new ConcurrentHashMap<>());
        }

        ConcurrentHashMap<Integer, ChunkInfo> chunkNoChunkInfo = environmentFilesRepLevel.get(request.getHeader().getFileId());
        if (!chunkNoChunkInfo.containsKey(request.getHeader().getChunkNo())) {
            chunkNoChunkInfo.put(request.getHeader().getChunkNo(), new ChunkInfo(-1, -1, 1));
        } else {
            ChunkInfo chunkInfo = chunkNoChunkInfo.get(request.getHeader().getChunkNo());
            chunkInfo.incrementReplicationLevel();
            chunkNoChunkInfo.put(request.getHeader().getChunkNo(), chunkInfo);
        }

        try {
            peer.getFileSystem().writeEnvironmentFilesRepLevelToDisk();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processStoreRequest(boolean initiatorPeer, Message request, ConcurrentHashMap<Integer, ChunkInfo> hashMap) {

        //Not storing this chunkNo
        if (hashMap.get(request.getHeader().getChunkNo()) == null)
            return;

        ConcurrentHashMap<Integer, CopyOnWriteArrayList<Integer>> storesReceivedHash = peer.getFileSystem().getStoresReceived().get(request.getHeader().getFileId());

        if (storesReceivedHash == null) {
            storesReceivedHash = new ConcurrentHashMap<>();
            storesReceivedHash.put(request.getHeader().getSenderId(), new CopyOnWriteArrayList<>());
        }

        CopyOnWriteArrayList<Integer> listOfReceivedChunkOfThatSender = storesReceivedHash.get(request.getHeader().getSenderId());

        if (listOfReceivedChunkOfThatSender == null) {
            CopyOnWriteArrayList<Integer> newList = new CopyOnWriteArrayList<>();
            storesReceivedHash.put(request.getHeader().getSenderId(), newList);
            listOfReceivedChunkOfThatSender = newList;
        }


        if (listOfReceivedChunkOfThatSender.contains(request.getHeader().getChunkNo()))
            return;

        //Add the chunk No to the record
        listOfReceivedChunkOfThatSender.add(request.getHeader().getChunkNo());

        ChunkInfo chunkInfo = hashMap.get(request.getHeader().getChunkNo());
        chunkInfo.incrementReplicationLevel();
        hashMap.put(request.getHeader().getChunkNo(), chunkInfo);

        if (initiatorPeer) {
            //For protocol 1.1
            if (!peer.getFileSystem().getPeerThatStoreAChunk().get(request.getHeader().getFileId()).contains(request.getHeader().getSenderId())) {
                peer.getFileSystem().getPeerThatStoreAChunk().get(request.getHeader().getFileId()).add(request.getHeader().getSenderId());
                try {
                    peer.getFileSystem().writePeerStoringChunksToDisk();
                } catch (IOException e) {
                    log.error("Error storing to disk The updated list of chunks storing the files");
                }
            }

            try {
                peer.getFileSystem().writeInternalFileMetadataToDisk();
            } catch (IOException e) {
                log.error("Error while registering an increment of the perceivedReplication");
            }

        } else {
            try {
                peer.getFileSystem().writeExternalFileChunksMetadataToDisk();
            } catch (IOException e) {
                log.error("Error while registering an increment of the perceivedReplication");
            }
        }
    }

    public void receivedChunk(Message request) {
        if (peer.getId() == request.getHeader().getSenderId())
            return;

        if (peer.getFileSystem().getFileRestorers().get(request.getHeader().getFileId()) == null) {
            CopyOnWriteArrayList<Integer> chunkListenedList = peer.getFileSystem().getChunksListened().get(request.getHeader().getFileId());

            if (chunkListenedList != null) {
                chunkListenedList.add(request.getHeader().getChunkNo());
                peer.getFileSystem().getChunksListened().put(request.getHeader().getFileId(), chunkListenedList);
            }
        }
    }

    public void receivedGetChunk(Message request) {

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        //Remove Any Content Related to chunk listened before for that pair fileId-chunkNo
        CopyOnWriteArrayList<Integer> chunkListenedList = peer.getFileSystem().getChunksListened().get(request.getHeader().getFileId());

        if (chunkListenedList == null) {
            chunkListenedList = new CopyOnWriteArrayList<>();
            peer.getFileSystem().getChunksListened().put(request.getHeader().getFileId(), chunkListenedList);
        }

        List<Integer> elementsToRemove = new ArrayList<>();

        for (Integer integer : chunkListenedList) {
            if (integer == request.getHeader().getChunkNo())
                elementsToRemove.add(integer);
        }

        chunkListenedList.removeAll(elementsToRemove);

        ConcurrentHashMap<Integer, ChunkInfo> storedHashMap = peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId());

        //No chunks for that file stored
        if (storedHashMap == null) {
            return;
        }

        //Dont have that particular chunk
        if (storedHashMap.get(request.getHeader().getChunkNo()) == null) {
            return;
        }

        Message reply = new Message(new Header(protocol.getVersion(), peer.getId(), request.getHeader().getFileId(), request.getHeader().getChunkNo(), "CHUNK", request.getHeader().getTcpPort()));

        protocol.sendChunk(reply);
    }

    public void receivedDelete(Message request) {
        if (peer.getId() == request.getHeader().getSenderId())
            return;

        if (peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId()) == null) {
            protocol.sendDeleteAck(request);
            return;
        }

        try {
            peer.getFileSystem().deleteChunks(request.getHeader().getFileId());
            protocol.sendDeleteAck(request);
            log.info("Deleted all content about that file of the system");
        } catch (IOException e) {
            log.error("Error Deleting Chunks");
            return;
        }
    }

    public void receivedPutChunk(Message request) {

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        try {
            Thread.sleep((long) (new Random().nextFloat() * 2000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ConcurrentHashMap<Integer, ChunkInfo> external = peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId());

        //Already Processing this request. If a retransmission occurs
        if (external != null && external.get(request.getHeader().getChunkNo()) != null) {
            log.info("Already dealing with this chunk");
            return;
        }

        int occupiedSpace = peer.getFileSystem().getOccupiedSpace();
        int dataSize = request.getBody().length;

        int spaceToStore = occupiedSpace + dataSize;
        if (peer.getFileSystem().getCapacity() < spaceToStore) {
            log.info("Can't store the chunk " + request.getHeader().getChunkNo() + " - no enough free space on disk, missing " + (spaceToStore - peer.getFileSystem().getCapacity()) + " bytes");
            return;
        }

        if (desiredReplicationLevelAchieved(request.getHeader().getFileId(), request.getHeader().getChunkNo(), request.getHeader().getDesiredReplicationLevel())) {
            log.info("Not storing chunk " + request.getHeader().getChunkNo() + " - desired replication level already achieved");
            return;
        }

        try {
            peer.getFileSystem().storeChunk(request);
            log.info("Storing chunk number " + request.getHeader().getChunkNo());
        } catch (IOException e) {
            log.error("Error Storing Chunk");
            return;
        }

        protocol.sendStored(request);
    }

    private boolean desiredReplicationLevelAchieved(String fileId, int chunkNo, int desiredReplicationLevel) {
        if (!peer.getFileSystem().getEnvironmentFilesRepLevel().containsKey(fileId)) {
            return false;
        }
        if (!peer.getFileSystem().getEnvironmentFilesRepLevel().get(fileId).containsKey(chunkNo)) {
            return false;
        }

        int perceivedRepLvl = peer.getFileSystem().getEnvironmentFilesRepLevel().get(fileId).get(chunkNo).getPerceivedReplicationLevel();

        return perceivedRepLvl >= desiredReplicationLevel + BIAS;
    }

    public void receivedHeartbeat(Message message) {

        //Update alivePeers/ delete dead ones only when getting the heartbeat
        //from myself in order to optimize computation
        if (peer.getId() == message.getHeader().getSenderId()) {
            removeDeadPeers();
            return;
        }

        ByteBuffer buffer = ByteBuffer.wrap(message.getBody());
        int peerFreeSpace = buffer.getInt();

        ConcurrentHashMap<Integer, PeerStatus> alivePeers = peer.getFileSystem().getAlivePeers();

        PeerStatus peerStatus = new PeerStatus(peerFreeSpace, System.currentTimeMillis());
        int peerId = message.getHeader().getSenderId();

        alivePeers.put(peerId, peerStatus);

        for (String fileIdWaitingDeleteAck : peer.getFileSystem().getFileDeletedWaitingForAck().keySet()) {
            CopyOnWriteArrayList<Integer> listOfPeerMissingAck = peer.getFileSystem().getFileDeletedWaitingForAck().get(fileIdWaitingDeleteAck);
            if (listOfPeerMissingAck.contains(peerId)) {
                log.info("Sending new DELETE to peer " + peerId + " because his ACK DELETE was not received");
                protocol.delete(fileIdWaitingDeleteAck);
            }
        }
    }

    private void removeDeadPeers() {
        long now = System.currentTimeMillis();
        List<Integer> deadPeerIds = new ArrayList<>();

        ConcurrentHashMap<Integer, PeerStatus> alivePeers = peer.getFileSystem().getAlivePeers();
        for (Map.Entry<Integer, PeerStatus> peerIdPeerStatus : alivePeers.entrySet()) {
            if (peerIdPeerStatus.getValue().getLastUpdatedAt() < now - ALIVE_PEERS_THRESHOLD) {
                deadPeerIds.add(peerIdPeerStatus.getKey());
            }
        }

        for (Integer deadPeerId : deadPeerIds) {
            alivePeers.remove(deadPeerId);
            log.info("Removed dead peer with id " + deadPeerId);
        }
    }

    public void receivedDeleteAck(Message request) {

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        CopyOnWriteArrayList<Integer> list = peer.getFileSystem().getFileDeletedWaitingForAck().get(request.getHeader().getFileId());

        if (list == null)
            return;

        if (list.contains(request.getHeader().getSenderId())) {
            list.remove((Integer) request.getHeader().getSenderId());
            peer.getFileSystem().getFileDeletedWaitingForAck().put(request.getHeader().getFileId(), list);
        }

        if (list.isEmpty())
            peer.getFileSystem().getFileDeletedWaitingForAck().remove(request.getHeader().getFileId());

        try {
            peer.getFileSystem().writeFileDeletedWaitingForAckToDisk();
        } catch (IOException e) {
            log.error("Unable to store in disk the update in Delete ACK received");
            return;
        }
    }

    // region helpers
    private boolean chunkBackedUp(String fileId, int chunkNo) {
        ChunkInfo chunkInfo = peer.getFileSystem().getInternalFiles().get(fileId).get(chunkNo);
        boolean isBackedUp = chunkInfo.getPerceivedReplicationLevel() == chunkInfo.getDesiredReplicationLevel();
        if (isBackedUp) {
            chunkInfo.setSize(Integer.MAX_VALUE);
        }
        return isBackedUp;
    }

    private boolean allChunksStored(String fileId) {
        for (ChunkInfo chunkInfo : peer.getFileSystem().getInternalFiles().get(fileId).values()) {
            if (chunkInfo.getSize() != Integer.MAX_VALUE) {
                return false;
            }
        }
        return true;
    }
    // endregion
}
