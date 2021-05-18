package client;

import peer.remote.RemoteInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * This class serve as test interface to run the code.
 */
public class TestApp {
    private static RemoteInterface initiatorPeer;
    private static Registry registry;

    /**
     * @param args "Usage: <peer_ap> BACKUP|RESTORE|DELETE|RECLAIM|STATE [<opnd_1> [<optnd_2]]"
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Incorrect Number of arguments");
            return;
        }
        try {
            registry = LocateRegistry.getRegistry();
            initiatorPeer = (RemoteInterface) registry.lookup(args[0]);
            switch (args[1]) {
                case "BACKUP":
                    handleBackup(args);
                    break;
                case "RESTORE":
                    handleRestore(args);
                    break;
                case "DELETE":
                    handleDelete(args);
                    break;
                case "RECLAIM":
                    handleReclaim(args);
                    break;
                case "STATE":
                    handleState();
                    break;
                default:
                    System.err.println("Invalid Operation");
                    break;
            }

        } catch (RemoteException e) {
            e.printStackTrace();
            System.err.println("Error in the RMI process registration");
        } catch (NotBoundException e) {
            System.err.println("Peer not bound!");
        }

    }

    private static void handleBackup(String[] args) {
        if (args.length < 4) {
            System.err.println("Invalid Backup Request");
            return;
        }
        try {
            initiatorPeer.backup(args[2], Integer.parseInt(args[3]));
        } catch (RemoteException e) {
            System.err.println("Error Executing The remote backup request");
            e.printStackTrace();
            return;
        }
    }

    private static void handleRestore(String[] args) {
        try {
            initiatorPeer.restore(args[2]);
        } catch (RemoteException e) {
            System.err.println("Error Executing The remote restore request");
            e.printStackTrace();
            return;
        }

    }

    private static void handleDelete(String[] args) {
        try {
            initiatorPeer.delete(args[2]);
        } catch (RemoteException e) {
            System.err.println("Error Executing The remote delete request");
            e.printStackTrace();
        }
    }

    private static void handleReclaim(String[] args) {
        try {
            initiatorPeer.reclaim(Integer.parseInt(args[2]));
        } catch (RemoteException e) {
            System.err.println("Error Executing The remote reclaim request");
            e.printStackTrace();
        }
    }

    private static void handleState() {
        try {
            System.out.println(initiatorPeer.state());
        } catch (RemoteException e) {
            System.err.println("Error Executing The remote state request");
            e.printStackTrace();
        }
    }
}
