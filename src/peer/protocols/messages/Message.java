package peer.protocols.messages;

import peer.utils.Constants;

import java.io.Serializable;
import java.net.DatagramPacket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Message implements Serializable {
    private Header header;
    private byte[] body;

    public Message(Header header) {
        this.header = header;
    }

    public Message(Header header, byte[] body) {
        this.header = header;
        this.body = body;
    }

    /**
     * This method translate a raw UDP request to a high level Message human friendly
     *
     * @param request Raw request received in the multicast channel
     */
    public Message(DatagramPacket request) {

        int counterBytesHeader = 0;

        byte[] cleanArray = Arrays.copyOfRange(request.getData(), 0, request.getLength());

        String string = new String(cleanArray, StandardCharsets.US_ASCII);

        String[] results = string.split(Constants.CRLF_STR, 4);

        String[] headerArray = null;


        if (results.length == 1)
            headerArray = Arrays.copyOfRange(results, 0, 1);
        else if (results[1].equals("")) {
            headerArray = Arrays.copyOfRange(results, 0, 2);
        } else if (results[2].equals(""))
            headerArray = Arrays.copyOfRange(results, 0, 3);

        if (headerArray == null) {
            System.err.println("No Header Line");
            return;
        }

        for (String arrayLine : headerArray) {
            counterBytesHeader = counterBytesHeader + arrayLine.getBytes(StandardCharsets.US_ASCII).length;
            counterBytesHeader = counterBytesHeader + Constants.CRLF_STR.getBytes(StandardCharsets.US_ASCII).length;
        }

        this.header = new Header(headerArray);

        //Last Index must always be the body
        if (results.length > 2 && !results[results.length - 1].equals(""))
            this.body = Arrays.copyOfRange(request.getData(), counterBytesHeader, request.getLength());


    }

    /**
     * @param message
     */
    public Message(Message message) {
        this.header = new Header(message.getHeader());

        if (message.getBody() != null)
            this.body = Arrays.copyOf(message.getBody(), message.getBody().length);
    }

    /**
     * @return Converts a high level message to a low level representation understandable by the socket level
     */
    public byte[] toByteArray() {

        byte[] byteMessage;
        String messageStr = header.getHeaderString();
        byte[] headerArray = messageStr.getBytes(StandardCharsets.US_ASCII);

        if (body != null) {
            byteMessage = new byte[headerArray.length + body.length];
            System.arraycopy(headerArray, 0, byteMessage, 0, headerArray.length);
            System.arraycopy(body, 0, byteMessage, headerArray.length, body.length);
        } else {
            byteMessage = new byte[headerArray.length];
            System.arraycopy(headerArray, 0, byteMessage, 0, headerArray.length);
        }
        return byteMessage;

    }

    public Header getHeader() {
        return header;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
