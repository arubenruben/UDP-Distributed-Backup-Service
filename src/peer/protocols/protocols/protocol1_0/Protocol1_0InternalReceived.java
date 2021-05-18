package peer.protocols.protocols.protocol1_0;

import peer.Peer;
import peer.filesystem.ChunkInfo;
import peer.filesystem.FileInfo;
import peer.filesystem.FileRestorer;
import peer.protocols.messages.Header;
import peer.protocols.messages.Message;
import peer.utils.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
/**
 * This class encapsulates the logic behind all income requests of the protocol 1.0
 */
public class Protocol1_0InternalReceived {

    private final Protocol1_0 protocol;
    private final Peer peer;
    private final Logger log;

    Protocol1_0InternalReceived(Protocol1_0 protocol, Peer peer, Logger log) {
        this.protocol = protocol;
        this.peer = peer;
        this.log = log;
    }

    public void receivedPutChunk(Message request) {

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        ConcurrentHashMap<Integer, ChunkInfo> external = peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId());

        //Already Processing this request. If a retransmission occurs
        if (external != null && external.get(request.getHeader().getChunkNo()) != null) {
            log.info("Already dealing with this chunk");
            return;
        }

        int occupiedSpace = peer.getFileSystem().getOccupiedSpace();
        int dataSize = request.getBody().length;

        if (peer.getFileSystem().getCapacity() < (occupiedSpace + dataSize)) {
            log.info("Can't store the chunk " + request.getHeader().getChunkNo() + " - no enough free space on disk");
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

        try {
            byte[] buffer = peer.getFileSystem().readChunk(request.getHeader().getFileId(), request.getHeader().getChunkNo());
            Message reply = new Message(new Header(protocol.getVersion(), peer.getId(), request.getHeader().getFileId(), request.getHeader().getChunkNo(), "CHUNK"), buffer);
            protocol.sendChunk(reply);
            log.info("Sending the chunk no " + request.getHeader().getChunkNo());
        } catch (IOException e) {
            log.error("Unable to Read Chunk from the filesystem");
            return;
        }
    }

    public void receivedChunk(Message request) {

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        //I'm restoring this file
        if (peer.getFileSystem().getFileRestorers().get(request.getHeader().getFileId()) != null) {

            FileRestorer fileRestorer = peer.getFileSystem().getFileRestorers().get(request.getHeader().getFileId());

            FileInfo fileInfo = null;

            Collection<FileInfo> fileInfoList = peer.getFileSystem().getFilenameFileInfo().values();

            for (FileInfo iterator : fileInfoList) {
                if (iterator.getFileId().equals(request.getHeader().getFileId())) {
                    fileInfo = iterator;
                    break;
                }
            }

            if (fileInfo == null) {
                log.error("No information Stored for that file");
                return;
            }

            fileRestorer.collectChunk(request);

            log.info("Restored chunk number " + request.getHeader().getChunkNo() + " from peer " + request.getHeader().getSenderId());
            log.info("Restored " + fileRestorer.getNumberOfChunksStored() + "/" + fileInfo.getNumberOfChunks() + " chunks");

            //Process not yet completed
            if (fileRestorer.getNumberOfChunksStored() < fileInfo.getNumberOfChunks())
                return;

            try {
                peer.getFileSystem().restoreFile(fileInfo, fileRestorer);
                peer.getFileSystem().writeFileRestorerStateToDisk();
                log.info("File successfully restored");
            } catch (IOException e) {
                log.error("Error while restoring the file in disk");
                return;
            }

        } else {
            CopyOnWriteArrayList<Integer> chunkListenedList = peer.getFileSystem().getChunksListened().get(request.getHeader().getFileId());

            if (chunkListenedList != null) {
                chunkListenedList.add(request.getHeader().getChunkNo());
                peer.getFileSystem().getChunksListened().put(request.getHeader().getFileId(), chunkListenedList);
            }
        }
    }

    public void receivedDelete(Message request) {
        if (peer.getId() == request.getHeader().getSenderId())
            return;

        if (peer.getFileSystem().getExternalFiles().get(request.getHeader().getFileId()) == null) {
            return;
        }

        try {
            peer.getFileSystem().deleteChunks(request.getHeader().getFileId());

            log.info("Deleted all content about that file of the system");
        } catch (IOException e) {
            log.error("Error Deleting Chunks");
        }
    }

    public void receivedStored(Message request) {
        String fileId = request.getHeader().getFileId();

        if (peer.getId() == request.getHeader().getSenderId())
            return;

        ConcurrentHashMap<Integer, ChunkInfo> fileBackupHash = peer.getFileSystem().getInternalFiles().get(fileId);

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
        ConcurrentHashMap<Integer, ChunkInfo> storedHash = peer.getFileSystem().getExternalFiles().get(fileId);

        if (storedHash != null) {
            processStoreRequest(false, request, storedHash);
            return;
        }
    }

    private boolean chunkBackedUp(String fileId, int chunkNo) {
        ChunkInfo chunkInfo = peer.getFileSystem().getInternalFiles().get(fileId).get(chunkNo);
        boolean isBackedUp = chunkInfo.getPerceivedReplicationLevel() == chunkInfo.getDesiredReplicationLevel();
        if (isBackedUp){
            chunkInfo.setSize(Integer.MAX_VALUE);
        }
        return isBackedUp;
    }

    private boolean allChunksStored(String fileId) {
        for (ChunkInfo chunkInfo : peer.getFileSystem().getInternalFiles().get(fileId).values()) {
            if (chunkInfo.getSize() != Integer.MAX_VALUE){
                return false;
            }
        }
        return true;
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

        try {
            if (initiatorPeer) {
                peer.getFileSystem().writeInternalFileMetadataToDisk();
            } else {
                peer.getFileSystem().writeExternalFileChunksMetadataToDisk();
            }
        } catch (IOException e){
            log.error("Error while registering an increment of the perceivedReplication");
        }
    }
}
