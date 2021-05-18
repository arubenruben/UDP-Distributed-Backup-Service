package peer.protocols.reply_worker_strategy;

import peer.protocols.protocols.Protocol;

import java.net.DatagramPacket;

public interface ReplyWorkerStrategy {
    void reply(DatagramPacket request, Protocol protocol);
}
