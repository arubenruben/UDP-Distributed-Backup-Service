package peer;

import peer.filesystem.FileSystem;
import peer.protocols.protocols.Protocol;
import peer.protocols.protocols.protocol1_0.Protocol1_0;
import peer.protocols.protocols.protocol1_1.Protocol1_1;
import peer.protocols.reply_worker_strategy.concrete_strategies.ReplyBackupWorker;
import peer.protocols.reply_worker_strategy.concrete_strategies.ReplyControlWorker;
import peer.protocols.reply_worker_strategy.concrete_strategies.ReplyRestoreWorker;
import peer.remote.RemoteInterface;
import peer.utils.Constants;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.security.InvalidParameterException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * This class instantiates a peer. The basis actor of this project
 */

public class Peer implements RemoteInterface {

    private final int id;
    private final ScheduledThreadPoolExecutor threadPool;
    private final String serviceAccessPoint;

    private Protocol protocol;
    private final FileSystem fileSystem;

    /**
     * @param protocolVersion    Version of the protocol
     * @param peerId             Id
     * @param serviceAccessPoint RMI Access point
     * @param MCIp               Multicast Control Channel IP
     * @param MCPort             Multicast Control Port
     * @param MDBIp              Multicast DataBackup IP
     * @param MDBPort            Multicast DataBackup Port
     * @param MDRIp              Multicast Data Restore IP
     * @param MDRPort            Multicast Data Restore Port
     * @throws IOException
     */
    public Peer(String protocolVersion, int peerId, String serviceAccessPoint, String MCIp, int MCPort, String MDBIp, int MDBPort, String MDRIp, int MDRPort) throws IOException {

        this.id = peerId;
        this.serviceAccessPoint = serviceAccessPoint;


        if (protocolVersion.equals("1.0"))
            this.protocol = new Protocol1_0(this, MCIp, MCPort, MDBIp, MDBPort, MDRIp, MDRPort);
        else if (protocolVersion.equals("1.1"))
            this.protocol = new Protocol1_1(this, MCIp, MCPort, MDBIp, MDBPort, MDRIp, MDRPort);
        else
            throw new InvalidParameterException("Invalid Protocol Version");

        this.fileSystem = new FileSystem(this.id);

        threadPool = new ScheduledThreadPoolExecutor(Constants.THREAD_POOL_SIZE);

        ReceiverThread controlChannelThread = new ReceiverThread(new ReplyControlWorker(), protocol, MCIp, MCPort, Constants.MAX_CONTROL_MSG_SIZE);
        ReceiverThread backupChannelThread = new ReceiverThread(new ReplyBackupWorker(), protocol, MDBIp, MDBPort, Constants.MAX_MESSAGE_SIZE);
        ReceiverThread restoreChannelThread = new ReceiverThread(new ReplyRestoreWorker(), protocol, MDRIp, MDRPort, Constants.MAX_MESSAGE_SIZE);

        new Thread(controlChannelThread).start();
        new Thread(backupChannelThread).start();
        new Thread(restoreChannelThread).start();

        System.out.println(protocol.getPeer().getId() + " Listening in all channels");

        new Thread(() -> heartbeatLoop()).start();
    }

    @Override
    public void backup(String filename, int replicationLevel) throws RemoteException {
        threadPool.execute(() -> protocol.backup(filename, replicationLevel));
    }

    @Override
    public void restore(String filename) throws RemoteException {
        threadPool.execute(() -> protocol.restore(filename));
    }

    @Override
    public void delete(String fileName) throws RemoteException {
        threadPool.execute(() -> {
            try {
                File file = new File(this.fileSystem.getInternalFilesDir().getAbsolutePath() + File.separator + fileName);
                protocol.delete(getFileSystem().generateFileId(file));
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error in Socket Delete");
            }
        });
    }

    @Override
    public void reclaim(int value) throws RemoteException {
        threadPool.execute(() -> protocol.reclaim(value));
    }

    @Override
    public String state() throws RemoteException {
        return protocol.state();
    }

    private void heartbeatLoop() {
        while (true) {
            try {
                threadPool.execute(() -> protocol.sendHeartbeat());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public ScheduledThreadPoolExecutor getThreadPool() {
        return threadPool;
    }

    public int getId() {
        return id;
    }

    public Remote getRemoteInterface() throws RemoteException {
        return UnicastRemoteObject.exportObject(this, 0);
    }

    public String getServiceAccessPoint() {
        return serviceAccessPoint;
    }
}
