package peer.peer_factory;


//args[]= numberPeers,MCIpAddress,MCfirstPort,MDBIpAddress,MDBfirstPort,MDRIpAddress,MDRfirstPort,
//The Ports will be assigned sequentially
//224.0.0.0 to 239.255.255.255 Range UDP Multicast

//args I runned: 10 224.0.0.1 5000 224.0.0.2 6000 224.0.0.2 7000

import peer.Peer;
import peer.utils.Constants;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;

/**
 * Class developed to launch peers. It could be launched in two different ways. One as a batch scenario, other to launch a single peer
 */
public class PeerFactory {
    /**
     * @param args <version> <peer_id> <svc_access_point> <mc_addr> <mc_port> <mdb_addr> <mdb_port> <mdr_addr> <mdr_port> <1>
     *             <version> <mc_addr> <mc_port> <mdb_addr> <mdb_port> <mdr_addr> <mdr_port> <(Number bigger than 1) Number of peers>
     */
    public static void main(String[] args) {
        Registry registry;

        if (args.length < 3)
            System.err.println("Incorrect Number of parameters");

        try {
            LocateRegistry.createRegistry(Constants.RMI_PORT);
        } catch (RemoteException ignored) {
        }
        try {
            registry = LocateRegistry.getRegistry(Constants.RMI_PORT);
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }


        int peerNumber = Integer.parseInt(args[args.length - 1]);

        if (peerNumber == 1) {

            String protocol = args[0];
            int peerId = Integer.parseInt(args[1]);
            String serviceAccessPoint = args[2];
            String MCIp = args[3];
            int MCPort = Integer.parseInt(args[4]);
            String MDBIp = args[5];
            int MDBPort = Integer.parseInt(args[6]);
            String MDRIp = args[7];
            int MDRPort = Integer.parseInt(args[8]);

            try {
                Peer peer = new Peer(protocol, peerId, serviceAccessPoint, MCIp, MCPort, MDBIp, MDBPort, MDRIp, MDRPort);
                registry.rebind(peer.getServiceAccessPoint(), peer.getRemoteInterface());
            } catch (IOException e) {
                System.err.println("Unable To Start thread id:" + peerId);
                e.printStackTrace();
            }

        } else if (peerNumber > 1) {

            for (int i = 0; i < peerNumber; i++) {

                int peerId = i;

                String protocol = args[0];
                String MCIp = args[1];
                int MCPort = Integer.parseInt(args[2]);
                String MDBIp = args[3];
                int MDBPort = Integer.parseInt(args[4]);
                String MDRIp = args[5];
                int MDRPort = Integer.parseInt(args[6]);
                String serviceAccessPoint = "peer" + peerId;
                try {
                    Peer peer = new Peer(protocol, peerId, serviceAccessPoint, MCIp, MCPort, MDBIp, MDBPort, MDRIp, MDRPort);
                    registry.rebind(peer.getServiceAccessPoint(), peer.getRemoteInterface());
                } catch (IOException e) {
                    System.err.println("Unable To Start thread id:" + peerId);
                    e.printStackTrace();
                }
            }
        }
    }
}
