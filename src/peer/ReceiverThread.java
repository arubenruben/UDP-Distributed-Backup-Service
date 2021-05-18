package peer;

import peer.protocols.protocols.Protocol;
import peer.protocols.reply_worker_strategy.ReplyWorkerStrategy;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * This class serves as multi thread worker. It listen the multicast channel in a own thread to achieve concurrency
 */
public class ReceiverThread implements Runnable {
    private final ReplyWorkerStrategy replyStrategy;
    private final Protocol protocol;

    private final MulticastSocket multicastSocket;
    private final int bufferSize;

    /**
     *
     * @param replyStrategy Worker Strategy to reply to requests
     * @param protocol Protocol version
     * @param ip IP to listen in the socket
     * @param port Port to listen in the socket
     * @param bufferSize Buffer size of the request collector
     * @throws IOException
     */
    public ReceiverThread(ReplyWorkerStrategy replyStrategy, Protocol protocol, String ip, int port, int bufferSize) throws IOException {

        this.replyStrategy = replyStrategy;
        this.protocol = protocol;

        this.bufferSize = bufferSize;

        multicastSocket = new MulticastSocket(port);
        multicastSocket.joinGroup(InetAddress.getByName(ip));

    }

    /**
     * Listens to requests
     */
    @Override
    public void run() {
        while (true) {
            try {
                byte[] buffer = new byte[bufferSize];
                DatagramPacket response = new DatagramPacket(buffer, bufferSize);

                multicastSocket.receive(response);
                reply(response, protocol);

            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Error Receiving message");
            }
        }
    }

    /**
     * Helper method to encapsulate the reply sending
     * @param response Response to send
     * @param protocol Protocol instance currently running
     */
    private void reply(DatagramPacket response, Protocol protocol) {
        replyStrategy.reply(response, protocol);
    }
}
