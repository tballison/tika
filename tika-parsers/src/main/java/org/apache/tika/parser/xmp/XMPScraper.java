package org.apache.tika.parser.xmp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.helpers.IOUtils;

/**
 * Created by tallison on 12/15/2015.
 */
public class XMPScraper {
    private static final byte[] PACKET_HEADER;
    private static final byte[] PACKET_END;
    private static final byte[] PACKET_TRAILER;

    static {
        PACKET_HEADER = "<?xpacket begin=".getBytes(StandardCharsets.US_ASCII);
        PACKET_END = "?>".getBytes(StandardCharsets.US_ASCII);
        PACKET_TRAILER = "<?xpacket".getBytes(StandardCharsets.US_ASCII);
    }

    public List<XMPPacket> scrape(InputStream is) throws IOException, XMPParseException {
        List<XMPPacket> packets = new ArrayList<>();
        while (extract(is, packets)) {}
        return packets;
    }

    private boolean extract(InputStream is, List<XMPPacket> packets) throws IOException, XMPParseException {
        boolean foundHeaderStart = find(is, PACKET_HEADER, null);
        if (! foundHeaderStart) {
            return false;
        }
        OutputStream header = new ByteArrayOutputStream();
        header.write(PACKET_HEADER);

        if (!find(is, PACKET_END, header)) {
            throw new XMPParseException("Couldn't find header end");
        }
        System.out.println(IOUtils.newStringFromBytes(((ByteArrayOutputStream)header).toByteArray(), StandardCharsets.US_ASCII.name()));
        OutputStream tmpBodyBuffer = new ByteArrayOutputStream();
        if (!find(is, PACKET_TRAILER, tmpBodyBuffer)) {
            throw new XMPParseException("Couldn't find packet trailer start");
        }


        byte[] tmpBody = ((ByteArrayOutputStream)tmpBodyBuffer).toByteArray();
        byte[] body = new byte[tmpBody.length-PACKET_TRAILER.length];
        System.arraycopy(tmpBody, 0, body, 0, tmpBody.length-PACKET_TRAILER.length);

        ByteArrayOutputStream trailer = new ByteArrayOutputStream();
        trailer.write(PACKET_TRAILER);

        if (! find(is, PACKET_END, trailer)) {
            throw new XMPParseException("Couldn't find packet trailer end");
        }

        XMPPacket packet = new XMPPacket(((ByteArrayOutputStream)header).toByteArray(),
                body, trailer.toByteArray());
        packets.add(packet);

        return true;
    }

    private boolean find(InputStream is, byte[] pattern, OutputStream buffer) throws IOException {
        int found = 0;
        int len = pattern.length;
        int b;
        while ((b = is.read()) >= 0) {
            if (buffer != null) {
                buffer.write(b);
            }
            if (b == pattern[found]) {
                found++;
                if (found == len) {
                    return true;
                }
            } else {
                found = 0;
            }
        }
        return false;

    }

}
