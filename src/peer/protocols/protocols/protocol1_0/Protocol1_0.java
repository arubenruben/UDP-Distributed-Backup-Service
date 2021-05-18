package peer.protocols.protocols.protocol1_0;

import peer.Peer;
import peer.filesystem.ChunkInfo;
import peer.filesystem.FileInfo;
import peer.filesystem.FileRestorer;
import peer.protocols.messages.Header;
import peer.protocols.messages.Message;
import peer.protocols.protocols.Protocol;
import peer.utils.Constants;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class Protocol1_0 extends Protocol {
    private final Protocol1_0InternalReceived protocolInternalReceived;
    private final Protocol1_0InternalSend protocolInternalSend;


    public Protocol1_0(Peer peer, String mcIp, int mcPort, String mdbIp, int mdbPort, String mdrIp, int mdrPort) {
        super(mcIp, mcPort, mdbIp, mdbPort, peer, "1.0");

        this.protocolInternalReceived = new Protocol1_0InternalReceived(this, peer, log);
        this.protocolInternalSend = new Protocol1_0InternalSend(this, peer, mcIp, mcPort, mdbIp, mdbPort, mdrIp, mdrPort, log);

    }

    public void backup(String filename, int replicationLevel) {

        List<byte[]> fileChunksList;
        String fileId;
        File file = peer.getFileSystem().openFile(filename);

        if (!file.exists()) {
            log.error("File does not exist");
            return;
        }

        try {
            fileId = peer.getFileSystem().generateFileId(file);
        } catch (NoSuchAlgorithmException | IOException e) {
            log.error("Error getting file Id");
            return;
        }

        // Check if the file is modified - if it is, delete it and backup, otherwise just backup
        if (peer.getFileSystem().getFilenameFileInfo().get(filename) != null) {

            FileInfo fileInfo = peer.getFileSystem().getFilenameFileInfo().get(filename);

            if (!fileInfo.getFileId().equals(fileId)) {
                delete(fileId);
                this.peer.getFileSystem().getFilenameFileInfo().remove(filename);
                log.info("File already has a back-up, but modified. Deleting the old one..");
            } else {
                log.info("Unmodified file already stored: " + filename);
                return;
            }
        }
        // continue with backing up
        log.info("Back-up of file " + filename + " in progress");

        try {
            fileChunksList = peer.getFileSystem().readFileInChunks(file);
        } catch (IOException e) {
            log.error("Error Opening File");
            return;
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error in the async operation of reading the file");
            return;
        }

        if (fileChunksList == null) {
            log.error("Buffer reference is null");
            return;
        }

        if (peer.getFileSystem().getInternalFiles().get(fileId) != null) {
            log.error("File Already Backup");
            return;
        }

        ConcurrentHashMap<Integer, ChunkInfo> replicationHashMap = new ConcurrentHashMap<>();

        //Ruben: I know, but I want this, in this way, otherwise i need to check for initializations
        for (int i = 0; i < fileChunksList.size(); i++)
            replicationHashMap.put(i, new ChunkInfo(-1, replicationLevel, 0));

        peer.getFileSystem().getInternalFiles().put(fileId, replicationHashMap);

        peer.getFileSystem().getFilenameFileInfo().put(filename, new FileInfo(filename, fileId, fileChunksList.size(), replicationLevel));

        peer.getFileSystem().getStoresReceived().put(fileId, new ConcurrentHashMap<>());

        //For protocol version 1.1
        peer.getFileSystem().getPeerThatStoreAChunk().put(fileId, new CopyOnWriteArrayList<>());
        ///

        for (int i = 0; i < fileChunksList.size(); i++) {
            Message message = new Message(new Header(version, peer.getId(), fileId, i, replicationLevel, "PUTCHUNK"), fileChunksList.get(i));
            peer.getThreadPool().schedule(() -> sendPutChunkHelper(message, 1), 0, TimeUnit.SECONDS);
        }
        try {
            peer.getFileSystem().writeInternalFileMetadataToDisk();
            peer.getFileSystem().writePeerStoringChunksToDisk();
        } catch (IOException e) {
            log.error("Unable to Save in Disk the Metadata of storing the new chunks");
        }

    }

    @Override
    public void restore(String filename) {

        FileInfo fileInfo = peer.getFileSystem().getFilenameFileInfo().get(filename);

        if (fileInfo == null) {
            log.error("File Info doesn't exist, cannot restore a non backed-up file");
            return;
        }

        String fileId = fileInfo.getFileId();

        FileRestorer restorer = peer.getFileSystem().getFileRestorers().get(fileId);

        if (restorer == null) {
            restorer = new FileRestorer();
            peer.getFileSystem().getFileRestorers().put(fileId, restorer);
        }

        if (fileInfo.getNumberOfChunks() == restorer.getNumberOfChunksStored()) {
            log.error("File is already restored");
            return;
        }

        for (int i = 0; i < fileInfo.getNumberOfChunks(); i++) {
            try {
                Header header = new Header(version, peer.getId(), fileId, i, "GETCHUNK");
                Message message = new Message(header);
                sendDatagram(message, mcIp, mcPort);
            } catch (IOException e) {
                log.error("Unable to send the GETCHUNK requests for restore");
            }
        }

    }

    @Override
    public void delete(String fileId) {

        for (int i = 0; i < Constants.ATTEMPTS_TO_DELETE; i++) {
            try {
                Message message = new Message(new Header(version, peer.getId(), fileId, "DELETE"));
                sendDatagram(message, mdbIp, mdbPort);
            } catch (Exception e) {
                log.error("Error Deleting Chunks");
            }
        }

        ConcurrentHashMap<String, FileInfo> fileNameFileInfo = peer.getFileSystem().getFilenameFileInfo();
        String filename = "";

        for (FileInfo fileInfo : fileNameFileInfo.values()) {
            if (fileInfo.getFileId().equals(fileId)) {
                filename = fileInfo.getFileName();
            }
        }

        fileNameFileInfo.remove(filename);
        peer.getFileSystem().getInternalFiles().remove(fileId);

        try {
            peer.getFileSystem().writeInternalFileMetadataToDisk();
        } catch (IOException e) {
            log.error("Error writing changes to disk");
        }

    }

    /**
     * Only is implemented in the 1.1 version
     * @param request
     */
    @Override
    public void receivedDeleteAck(Message request) {

    }

    @Override
    public void receivedPutChunk(Message request) {
        protocolInternalReceived.receivedPutChunk(request);
    }

    @Override
    public void receivedStored(Message request) {
        protocolInternalReceived.receivedStored(request);
    }

    @Override
    public void receivedDelete(Message request) {
        protocolInternalReceived.receivedDelete(request);
    }


    @Override
    public void receivedGetChunk(Message request) {
        protocolInternalReceived.receivedGetChunk(request);
    }

    @Override
    public void receivedChunk(Message request) {
        protocolInternalReceived.receivedChunk(request);
    }

    @Override
    public void sendChunk(Message request) {
        protocolInternalSend.sendChunk(request);
    }


}
