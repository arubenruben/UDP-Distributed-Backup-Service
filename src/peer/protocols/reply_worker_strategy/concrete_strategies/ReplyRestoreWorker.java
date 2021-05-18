package peer.protocols.reply_worker_strategy.concrete_strategies;

import peer.protocols.messages.Message;
import peer.protocols.protocols.Protocol;
import peer.protocols.reply_worker_strategy.ReplyWorkerStrategy;

import java.net.DatagramPacket;

public class ReplyRestoreWorker implements ReplyWorkerStrategy {
    /**
     * Dispatcher Strategy for the packets received in Restore Multicast Channel
     *
     * @param request  Request received
     * @param protocol Protocol currently in use
     */
    @Override
    public void reply(DatagramPacket request, Protocol protocol) {
        try {
            Message message = new Message(request);
            protocol.receivedChunk(message);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Unable to parse the request data");

        }
    }
}
