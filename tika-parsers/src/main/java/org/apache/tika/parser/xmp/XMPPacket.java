package org.apache.tika.parser.xmp;

/**
 * Created by tallison on 12/15/2015.
 */
public class XMPPacket {
    private final byte[] header;
    private final byte[] payload;

    public XMPPacket(byte[] header, byte[] payload) {
        this.header = header;
        this.payload = payload;
    }

    public byte[] getHeader() {
        return header;
    }

    public byte[] getPayload() {
        return payload;
    }
}
