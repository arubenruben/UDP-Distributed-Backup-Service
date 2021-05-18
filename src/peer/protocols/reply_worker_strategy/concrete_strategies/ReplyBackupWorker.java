package peer.protocols.reply_worker_strategy.concrete_strategies;

import peer.protocols.messages.Message;
import peer.protocols.protocols.Protocol;
import peer.protocols.reply_worker_strategy.ReplyWorkerStrategy;

import java.net.DatagramPacket;


public class ReplyBackupWorker implements ReplyWorkerStrategy {
    /**
     * Dispatcher Strategy for the packets received in Backup Multicast Channel
     *
     * @param request  Request received
     * @param protocol Protocol currently in use
     */
    @Override
    public void reply(DatagramPacket request, Protocol protocol) {
        try {
            Message requestMessage = new Message(request);
            switch (requestMessage.getHeader().getSubProtocol()) {
                case "PUTCHUNK":
                    protocol.receivedPutChunk(requestMessage);
                    break;
                case "DELETE":
                    protocol.receivedDelete(requestMessage);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error Constructing the Reply");
            return;
        }

    }
}
