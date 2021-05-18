package peer.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI Interface. The methods that every protocol should expose to their clients
 */
public interface RemoteInterface extends Remote {
    void backup(String filename, int replicationLevel) throws RemoteException;

    void restore(String filename) throws RemoteException;

    void delete(String filename) throws RemoteException;

    void reclaim(int value) throws RemoteException;

    String state() throws RemoteException;

}
