package org.apache.tika.parser.xmp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by tallison on 12/15/2015.
 */
public class XMPPacket {
    private final byte[] header;
    private final byte[] payload;
    private final byte[] trailer;

    public XMPPacket(byte[] header, byte[] payload, byte[] trailer) {
        this.header = header;
        this.payload = payload;
        this.trailer = trailer;
    }

    public byte[] getHeader() {
        return header;
    }

    public byte[] getPayload() {
        return payload;
    }

    public byte[] getTrailer() { return trailer; }

    public InputStream getFullPacket() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(header);
        bos.write(payload);
        bos.write(trailer);
        return new ByteArrayInputStream(bos.toByteArray());
    }
}
