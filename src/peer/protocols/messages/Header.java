package peer.protocols.messages;

import peer.utils.Constants;

import java.io.Serializable;

public class Header implements Serializable {

    private String protocolVersion;
    private int senderId;
    private String fileId;
    private int chunkNo = -1;
    private Integer desiredReplicationLevel;
    private String subProtocol;
    private int tcpPort = -1;

    /**
     * @param protocolVersion
     * @param senderId
     * @param fileId
     * @param chunkNo
     * @param replicationLevel
     * @param subProtocol
     */
    public Header(String protocolVersion, int senderId, String fileId, int chunkNo, int replicationLevel, String subProtocol) {

        this.protocolVersion = protocolVersion;
        this.senderId = senderId;
        this.fileId = fileId;
        this.chunkNo = chunkNo;
        this.desiredReplicationLevel = replicationLevel;
        this.subProtocol = subProtocol;
    }

    /**
     * @param protocolVersion
     * @param senderId
     * @param fileId
     * @param chunkNo
     * @param subProtocol
     */
    public Header(String protocolVersion, int senderId, String fileId, int chunkNo, String subProtocol) {

        this.protocolVersion = protocolVersion;
        this.senderId = senderId;
        this.fileId = fileId;
        this.chunkNo = chunkNo;
        this.subProtocol = subProtocol;
    }

    /**
     * @param protocolVersion
     * @param senderId
     * @param fileId
     * @param subProtocol
     */
    public Header(String protocolVersion, int senderId, String fileId, String subProtocol) {
        this.protocolVersion = protocolVersion;
        this.senderId = senderId;
        this.fileId = fileId;
        this.subProtocol = subProtocol;
    }

    /**
     * @param protocolVersion
     * @param senderId
     * @param fileId
     * @param chunkNo
     * @param subProtocol
     * @param TCP_PORT
     */
    public Header(String protocolVersion, int senderId, String fileId, int chunkNo, String subProtocol, int TCP_PORT) {

        this.protocolVersion = protocolVersion;
        this.senderId = senderId;
        this.fileId = fileId;
        this.chunkNo = chunkNo;
        this.subProtocol = subProtocol;
        this.tcpPort = TCP_PORT;
    }

    /**
     * @param protocolVersion
     * @param senderId
     * @param subProtocol
     */
    public Header(String protocolVersion, int senderId, String subProtocol) {
        this.protocolVersion = protocolVersion;
        this.senderId = senderId;
        this.subProtocol = subProtocol;
    }

    /**
     * @param headerArray Array of raw data from a socket read. Parsing required
     */
    public Header(String[] headerArray) {

        if (headerArray.length == 0) {
            System.err.println("No header Line");
            return;
        }

        String[] headerFirstRow = headerArray[0].split(" ");

        switch (headerFirstRow[1]) {
            case "PUTCHUNK":
                if (headerFirstRow.length != 6)
                    return;
                this.protocolVersion = headerFirstRow[0];
                this.subProtocol = headerFirstRow[1];
                this.senderId = Integer.parseInt(headerFirstRow[2]);
                this.fileId = headerFirstRow[3];
                this.chunkNo = Integer.parseInt(headerFirstRow[4]);
                this.desiredReplicationLevel = Integer.parseInt(headerFirstRow[5]);
                break;
            case "GETCHUNK":
                this.protocolVersion = headerFirstRow[0];
                this.subProtocol = headerFirstRow[1];
                this.senderId = Integer.parseInt(headerFirstRow[2]);
                this.fileId = headerFirstRow[3];
                this.chunkNo = Integer.parseInt(headerFirstRow[4]);

                //For the protocol 1.1
                if (this.protocolVersion.equals("1.1"))
                    this.tcpPort = Integer.parseInt(headerArray[1]);

                break;

            case "STORED":
            case "CHUNK":
            case "REMOVED":

                this.protocolVersion = headerFirstRow[0];
                this.subProtocol = headerFirstRow[1];
                this.senderId = Integer.parseInt(headerFirstRow[2]);
                this.fileId = headerFirstRow[3];
                this.chunkNo = Integer.parseInt(headerFirstRow[4]);

                break;
            case "DELETE":
            case "DELETE_ACK":
                this.protocolVersion = headerFirstRow[0];
                this.subProtocol = headerFirstRow[1];
                this.senderId = Integer.parseInt(headerFirstRow[2]);
                this.fileId = headerFirstRow[3];
                break;
            case "HEARTBEAT":
                this.protocolVersion = headerFirstRow[0];
                this.subProtocol = headerFirstRow[1];
                this.senderId = Integer.parseInt(headerFirstRow[2]);
                break;
            default:
                System.err.println("Not implemented YET");
        }
    }

    public Header(Header header) {
        this.protocolVersion = header.getProtocolVersion();
        this.senderId = header.getSenderId();
        this.fileId = header.getFileId();

        if (header.chunkNo > -1)
            this.chunkNo = header.getChunkNo();

        if (header.desiredReplicationLevel != null)
            this.desiredReplicationLevel = header.getDesiredReplicationLevel();

        this.subProtocol = header.getSubProtocol();
        this.tcpPort = header.getTcpPort();
    }

    /**
     * @return Makes the conversion from a high level Message to a low level that the socket can send in the network
     */
    public String getHeaderString() {

        if (protocolVersion.equals("1.0")) {
            String stringAux = protocolVersion + " " + subProtocol + " " + senderId + " " + fileId;

            if (chunkNo >= 0)
                stringAux = stringAux + " " + chunkNo;

            if (desiredReplicationLevel != null)
                stringAux = stringAux + " " + desiredReplicationLevel;

            return stringAux + " " + Constants.CRLF_STR + Constants.CRLF_STR;
        } else if (protocolVersion.equals("1.1")) {
            String stringAux = protocolVersion + " " + subProtocol + " " + senderId + " " + fileId;

            if (chunkNo >= 0)
                stringAux = stringAux + " " + chunkNo;

            if (desiredReplicationLevel != null)
                stringAux = stringAux + " " + desiredReplicationLevel;

            if (tcpPort > -1) {
                stringAux = stringAux + Constants.CRLF_STR + tcpPort;
            }

            return stringAux + Constants.CRLF_STR + Constants.CRLF_STR;
        }
        return null;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public int getSenderId() {
        return senderId;
    }

    public String getFileId() {
        return fileId;
    }

    public int getChunkNo() {
        return chunkNo;
    }

    public int getDesiredReplicationLevel() {
        return desiredReplicationLevel;
    }

    public String getSubProtocol() {
        return subProtocol;
    }

    public int getTcpPort() {
        return tcpPort;
    }
}
