package peer.protocols.reply_worker_strategy.concrete_strategies;

import peer.protocols.messages.Message;
import peer.protocols.protocols.Protocol;
import peer.protocols.reply_worker_strategy.ReplyWorkerStrategy;

import java.net.DatagramPacket;

public class ReplyControlWorker implements ReplyWorkerStrategy {
    /**
     * Dispatcher Strategy for the packets received in Control Multicast Channel
     *
     * @param request  Request received
     * @param protocol Protocol currently in use
     */
    @Override
    public void reply(DatagramPacket request, Protocol protocol) {

        try {
            Message message = new Message(request);

            switch (message.getHeader().getSubProtocol()) {
                case "STORED":
                    protocol.receivedStored(message);
                    return;
                case "GETCHUNK":
                    protocol.receivedGetChunk(message);
                    return;
                case "REMOVED":
                    protocol.receivedRemoved(message);
                    return;
                case "HEARTBEAT":
                    protocol.receivedHeartbeat(message);
                    return;
                case "DELETE_ACK":
                    protocol.receivedDeleteAck(message);
                    return;

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error Constructing the Control Reply");
            return;
        }
    }
}
