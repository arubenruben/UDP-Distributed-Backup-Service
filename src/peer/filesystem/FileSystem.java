package peer.filesystem;

import peer.protocols.messages.Message;
import peer.utils.Constants;
import peer.utils.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This classes keeps the reference to all the data structures used in this project
 */
public class FileSystem {
    private final int peerId;
    private final Logger log;
    private int capacity;
    private AtomicInteger occupiedSpace;

    private final File internalFilesDir;
    private final File externalFilesDir;
    private final File metadataDirectory;
    private final File restoreDirectory;


    //I'm the source
    //FileId-(ChunkNo-Replication Level)
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> internalFiles;
    //-----
    //I'm the destination
    //FileId-ChunkNo
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> externalFiles;
    //-----
    //FileId-FileRestorer(Chunks)
    private ConcurrentHashMap<String, FileRestorer> fileRestorers;
    //-----
    //Filename-FileInfo
    private ConcurrentHashMap<String, FileInfo> filenameFileInfo;
    //-----
    //To help aborting if needed the process of CHUNK replies
    //FileId-ListenedChunks
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> chunksListened;
    //To help keep track of where and what I received a store for the STORED receiving
    //FileId-<SenderId-List of Chunk NO>
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, CopyOnWriteArrayList<Integer>>> storesReceived;
    //To be able to implement the backup enhancement, this map has to be introduced
    //FileId-ChunkNo-ChunkInfo (Replication levels)
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> environmentFilesRepLevel;
    //This hashmap stores all the alive peers
    private ConcurrentHashMap<Integer, PeerStatus> alivePeers;
    //File Id - (Chunk No-List<Integer>Peer Id)
    private ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> peerThatStoreAChunk;
    //FileId-PeerIds That didnt yet ack
    private ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> fileDeletedWaitingForAck;

    public FileSystem(int peerId) {
        this.peerId = peerId;
        this.log = new Logger(peerId);
        this.occupiedSpace = new AtomicInteger(0);

        String currentPath = System.getProperty("user.dir") + File.separator + "build" + File.separator + "peer" + File.separator + "filesystem" + File.separator + "storage" + File.separator + this.peerId;

        this.internalFilesDir = new File(currentPath + Constants.FILE_BASE_PATH);
        this.externalFilesDir = new File(currentPath + Constants.CHUNK_BASE_PATH);
        this.metadataDirectory = new File(currentPath + Constants.METADATA_PATH);
        this.restoreDirectory = new File(currentPath + Constants.RESTORE_PATH);
        this.internalFiles = new ConcurrentHashMap<>();
        this.filenameFileInfo = new ConcurrentHashMap<>();
        this.externalFiles = new ConcurrentHashMap<>();
        this.fileRestorers = new ConcurrentHashMap<>();
        this.chunksListened = new ConcurrentHashMap<>();
        this.storesReceived = new ConcurrentHashMap<>();
        this.environmentFilesRepLevel = bootstrapEnvironmentFilesRepLevel();
        this.peerThatStoreAChunk = new ConcurrentHashMap<>();
        this.alivePeers = new ConcurrentHashMap<>();
        this.fileDeletedWaitingForAck = new ConcurrentHashMap<>();

        this.capacity = bootstrapPeerCapacity();

        if (internalFilesDir.exists()) {
            bootstrapFilesDirectory();
            bootstrapPeersStoringChunks();
            bootstrapFileDeletedWaitingForAck();
        } else {
            internalFilesDir.mkdirs();
        }

        if (externalFilesDir.exists()) {
            bootstrapChunksDirectory();
            calculateOccupiedSpace();
        } else {
            externalFilesDir.mkdirs();
        }

        if (!metadataDirectory.exists())
            metadataDirectory.mkdirs();

        if (restoreDirectory.exists()) {
            bootstrapFileRestorers();
        } else {
            restoreDirectory.mkdirs();
        }

    }

    public File openFile(String filename) {
        String fullPath = internalFilesDir.getAbsolutePath() + File.separator + filename;
        return new File(fullPath);
    }

    /**
     * Reads the requested FILE in chunk items
     *
     * @param file
     * @return
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public List<byte[]> readFileInChunks(File file) throws IOException, ExecutionException, InterruptedException {
        List<byte[]> response = new ArrayList<>();
        int bytesMissingToRead = (int) file.length();
        int number_chunks = (int) Math.ceil((double) file.length() / Constants.MAX_CHUNK_SIZE);

        //If the file is soo small. The integer division return 0
        if (number_chunks == 0 && file.length() % Constants.MAX_CHUNK_SIZE != 0)
            number_chunks++;

        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.READ);

        for (int i = 0; i < number_chunks; i++) {
            int filePointOffset = Constants.MAX_CHUNK_SIZE * i;

            int bufferSize = Constants.MAX_CHUNK_SIZE;

            if (bytesMissingToRead < Constants.MAX_CHUNK_SIZE)
                bufferSize = bytesMissingToRead;

            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

            fileChannel.read(buffer, filePointOffset).get();
            buffer.flip();
            byte[] data = new byte[buffer.limit()];
            buffer.get(data);
            buffer.clear();
            response.add(data.clone());

            bytesMissingToRead = bytesMissingToRead - bufferSize;
        }

        if (file.length() % Constants.MAX_CHUNK_SIZE == 0)
            response.add(new byte[0]);

        return response;

    }

    /**
     * Write a Chunk to disk
     *
     * @param file
     * @param message
     * @throws IOException
     */
    public synchronized void writeChunkToDisk(File file, Message message) throws IOException {
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        ByteBuffer buffer = ByteBuffer.wrap(message.getBody());
        fileChannel.write(buffer, 0);
    }

    /**
     * Stored chunks record save to disk
     *
     * @throws IOException
     */
    public synchronized void writeExternalFileChunksMetadataToDisk() throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "stored", false))));
        outputStream.writeObject(externalFiles);
        outputStream.close();
    }

    /**
     * Stores the record of the backup files to disk
     *
     * @throws IOException
     */
    public synchronized void writeInternalFileMetadataToDisk() throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "backup", false))));
        outputStream.writeObject(internalFiles);
        outputStream.close();

        outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "backup_fileinfo", false))));
        outputStream.writeObject(filenameFileInfo);
        outputStream.close();

        writeFileRestorerStateToDisk();
    }

    /**
     * Keeps the information about the restoration state to disk
     *
     * @throws IOException
     */
    public synchronized void writeFileRestorerStateToDisk() throws IOException {
        ObjectOutputStream outputStream;
        outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "restored", false))));
        outputStream.writeObject(fileRestorers);
        outputStream.close();
    }

    /**
     * Information about which peer stores what chunks in which files to disk
     *
     * @throws IOException
     */
    public synchronized void writePeerStoringChunksToDisk() throws IOException {
        ObjectOutputStream outputStream;
        outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "peer_storing_chunks", false))));
        outputStream.writeObject(peerThatStoreAChunk);
        outputStream.close();
    }

    /**
     * Saves the records about files that didnt ack a delete yet to disk
     *
     * @throws IOException
     */
    public synchronized void writeFileDeletedWaitingForAckToDisk() throws IOException {
        ObjectOutputStream outputStream;
        outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "file_waiting_for_delete", false))));
        outputStream.writeObject(fileDeletedWaitingForAck);
        outputStream.close();
    }

    /**
     * Stores the value of disk capacity of other peers left in disk
     *
     * @throws IOException
     */
    public synchronized void writePeerCapacityToDisk() throws IOException {
        ObjectOutputStream outputStream;
        outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "peer_capacity", false))));
        outputStream.writeObject(capacity);
        outputStream.close();
    }

    /**
     * The perception of the environment save to disk
     *
     * @throws IOException
     */
    public synchronized void writeEnvironmentFilesRepLevelToDisk() throws IOException {
        ObjectOutputStream outputStream;
        outputStream = new ObjectOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadataDirectory.getAbsolutePath() + File.separator + "environment", false))));
        outputStream.writeObject(environmentFilesRepLevel);
        outputStream.close();
    }

    /**
     * Saves a chunk contained in a message packet to disk
     *
     * @param message
     * @throws IOException
     */
    public void storeChunk(Message message) throws IOException {
        ConcurrentHashMap<Integer, ChunkInfo> fileHashMap = externalFiles.get(message.getHeader().getFileId());

        if (fileHashMap == null)
            fileHashMap = new ConcurrentHashMap<>();

        fileHashMap.put(message.getHeader().getChunkNo(), new ChunkInfo(message.getBody().length, message.getHeader().getDesiredReplicationLevel(), 1));

        externalFiles.put(message.getHeader().getFileId(), fileHashMap);

        occupiedSpace.set(occupiedSpace.addAndGet(message.getBody().length));

        //Need to be here after hashmap update, since it may lead to inconsistency in the threads
        File newChunkPath = getChunkPath(message.getHeader().getFileId());

        if (!newChunkPath.exists())
            newChunkPath.mkdirs();

        writeChunkToDisk(new File(newChunkPath.getAbsolutePath() + File.separator + message.getHeader().getChunkNo()), message);
        writeExternalFileChunksMetadataToDisk();

    }

    /**
     * Reads chunks from disk
     *
     * @param fileId
     * @param chunkNo
     * @return
     * @throws IOException
     */
    public byte[] readChunk(String fileId, Integer chunkNo) throws IOException {


        File file = new File(getChunkPath(fileId).getAbsolutePath() + File.separator + chunkNo);

        if (!file.exists()) {
            System.err.println("File not found in the Filesystem");
            return null;
        }

        byte[] buffer = new byte[(int) file.length()];
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));
        inputStream.read(buffer, 0, buffer.length);
        inputStream.close();

        return buffer;
    }

    public void deleteEmptyFolders() {
        for (File folder : externalFilesDir.listFiles()) {
            if (folder.isDirectory() && folder.listFiles().length == 0) {
                folder.delete();
            }
        }

        List<String> emptyExtFiles = new ArrayList<>();
        for (Map.Entry<String, ConcurrentHashMap<Integer, ChunkInfo>> extFile : externalFiles.entrySet()) {
            if (extFile.getValue().size() == 0) {
                emptyExtFiles.add(extFile.getKey());
            }
        }

        for (String fileId : emptyExtFiles) {
            externalFiles.remove(fileId);
        }

        try {
            writeExternalFileChunksMetadataToDisk();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Deletes chunks from the file directory and memory
     *
     * @param fileId
     * @throws IOException
     */
    public void deleteChunks(String fileId) throws IOException {
        File folder = getChunkPath(fileId);
        File[] content = folder.listFiles();
        if (content != null) {
            for (File f : content) {
                if (!Files.isSymbolicLink(f.toPath())) {
                    f.delete();
                }
            }
        }
        folder.delete();

        log.info("Current occupied space: " + occupiedSpace);
        log.info("Deleting chunks...");

        for (ChunkInfo chunk : externalFiles.get(fileId).values()) {
            occupiedSpace.set(occupiedSpace.addAndGet(-chunk.getSize()));
        }


        log.info("Current occupied space: " + occupiedSpace);
        externalFiles.remove(fileId);
        log.info("Removed from external");

        writeExternalFileChunksMetadataToDisk();
    }

    /**
     * Delete the specific chunk from disk
     *
     * @param fileId
     * @param chunkNumber
     * @throws IOException
     */
    public void deleteChunk(String fileId, int chunkNumber) throws IOException {
        for (File file : getExternalFilesDir().listFiles()) {
            if (file.getName().equals(fileId)) {
                for (File chunk : file.listFiles()) {
                    if (chunk.getName().equals(String.format("%d", chunkNumber))) {
                        if (chunk.delete()) {
                            log.info("Successfully removed chunk " + chunkNumber + " with fileId " + fileId);
                        } else {
                            log.warn("Unable to remove the chunk " + chunkNumber + " with fileId " + fileId);
                        }
                    }
                }
            }
        }
        occupiedSpace.set(occupiedSpace.addAndGet(-externalFiles.get(fileId).get(chunkNumber).getSize()));
        log.info("Current space usage " + occupiedSpace + " out of " + capacity);
        externalFiles.get(fileId).remove(chunkNumber);

        writeExternalFileChunksMetadataToDisk();
    }

    /**
     * Does the process of restoring a file
     *
     * @param fileInfo
     * @param fileRestorer
     * @throws IOException
     */
    public void restoreFile(FileInfo fileInfo, FileRestorer fileRestorer) throws IOException {

        File file = new File(restoreDirectory.getAbsolutePath() + File.separator + fileInfo.getFileName());
        FileOutputStream outputStream = new FileOutputStream(file);

        for (int i = 0; i < fileInfo.getNumberOfChunks(); i++) {
            byte[] data = fileRestorer.getChunks().get(i);
            outputStream.write(data);
        }

        outputStream.close();

    }

    //Bootstraps

    /**
     * Loads the information about the files backup from disk
     */
    private void bootstrapFilesDirectory() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "backup"))));
            internalFiles = (ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>>) inputStream.readObject();
            inputStream.close();

            inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "backup_fileinfo"))));
            filenameFileInfo = (ConcurrentHashMap<String, FileInfo>) inputStream.readObject();
            inputStream.close();


        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads the information about the files restored from disk
     */
    private void bootstrapFileRestorers() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "restored"))));
            ConcurrentHashMap<String, FileRestorer> restorersFromMeta = (ConcurrentHashMap<String, FileRestorer>) inputStream.readObject();
            inputStream.close();

            // fileRestorers = restorers from metadata, still present in the file system

            // collect internal files
            HashMap<String, File> internal = new HashMap<>();
            for (File file : internalFilesDir.listFiles()) {
                internal.put(file.getName(), file);
            }

            // keep only files present in the system
            Set<String> keep = new HashSet<>();
            for (File file : restoreDirectory.listFiles()) {
                if (internal.containsKey(file.getName())) {
                    keep.add(generateFileId(internal.get(file.getName())));
                }
            }

            ConcurrentHashMap<String, FileRestorer> presentRestorers = new ConcurrentHashMap<>();
            for (String fileId : restorersFromMeta.keySet()) {
                if (keep.contains(fileId)) {
                    presentRestorers.put(fileId, restorersFromMeta.get(fileId));
                }
            }

            fileRestorers = presentRestorers;

        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads the chunks stored info from disk
     */
    private void bootstrapChunksDirectory() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "stored"))));
            externalFiles = (ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>>) inputStream.readObject();
            inputStream.close();
        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads the info about which peer stores each chunk from disk
     */
    private void bootstrapPeersStoringChunks() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "peer_storing_chunks"))));
            peerThatStoreAChunk = (ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>) inputStream.readObject();
            inputStream.close();
        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads the info about peers missing their delete acks from disk
     */
    private void bootstrapFileDeletedWaitingForAck() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "file_waiting_for_delete"))));
            fileDeletedWaitingForAck = (ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>) inputStream.readObject();
            inputStream.close();
        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads the other peers capacity values from disk
     *
     * @return
     */
    private int bootstrapPeerCapacity() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "peer_capacity"))));
            int cap = (Integer) inputStream.readObject();
            inputStream.close();
            return cap;
        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Constants.MAX_DISK_CAPACITY;
    }

    /**
     * Loads the File replication level perception from disk
     *
     * @return
     */
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> bootstrapEnvironmentFilesRepLevel() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new DataInputStream(new BufferedInputStream(new FileInputStream(metadataDirectory + File.separator + "environment"))));
            ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> env = (ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>>) inputStream.readObject();
            inputStream.close();
            return env;
        } catch (FileNotFoundException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ConcurrentHashMap<>();
    }

    /**
     * Calculates the space that chunks are occupying at a given moment in the filesystem
     */
    private void calculateOccupiedSpace() {
        for (String fileId : externalFiles.keySet()) {
            for (Integer chunkNum : externalFiles.get(fileId).keySet()) {
                occupiedSpace.set(occupiedSpace.addAndGet(externalFiles.get(fileId).get(chunkNum).getSize()));
            }
        }
    }

    /**
     * Generates the file Id for the given File
     *
     * @param file
     * @return
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    //GenerateFileId
    public String generateFileId(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        String fileMetadataString = file.getName() + file.lastModified() + Files.getOwner(Paths.get(file.getAbsolutePath()));
        byte[] hash = digest.digest(fileMetadataString.getBytes(StandardCharsets.US_ASCII));

        StringBuilder sb = new StringBuilder();

        for (byte b : hash)
            sb.append(String.format("%02X", b));

        return sb.reverse().toString();

    }

    //Getters
    private File getChunkPath(String fileId) {
        return new File(externalFilesDir.getAbsolutePath() + File.separator + fileId + File.separator);
    }

    public File getInternalFilesDir() {
        return internalFilesDir;
    }

    public File getExternalFilesDir() {
        return externalFilesDir;
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> getInternalFiles() {
        return internalFiles;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> getExternalFiles() {
        return externalFiles;
    }

    public ConcurrentHashMap<String, FileRestorer> getFileRestorers() {
        return fileRestorers;
    }

    public ConcurrentHashMap<String, FileInfo> getFilenameFileInfo() {
        return filenameFileInfo;
    }

    public ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> getChunksListened() {
        return chunksListened;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, CopyOnWriteArrayList<Integer>>> getStoresReceived() {
        return storesReceived;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;

        try {
            writePeerCapacityToDisk();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getOccupiedSpace() {
        return occupiedSpace.get();
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ChunkInfo>> getEnvironmentFilesRepLevel() {
        return environmentFilesRepLevel;
    }

    public ConcurrentHashMap<Integer, PeerStatus> getAlivePeers() {
        return alivePeers;
    }

    public ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> getFileDeletedWaitingForAck() {
        return fileDeletedWaitingForAck;
    }

    public ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> getPeerThatStoreAChunk() {
        return peerThatStoreAChunk;
    }

    /**
     * Converts the Id of given file to file name. Search the requests
     *
     * @param fileId
     * @return
     */
    public String idToFilename(String fileId) {
        for (Map.Entry<String, FileInfo> filenameInfo : filenameFileInfo.entrySet()) {
            if (filenameInfo.getValue().getFileId().equals(fileId)) {
                return filenameInfo.getKey();
            }
        }
        return fileId;
    }
}

