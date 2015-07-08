/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* $Id: XMPPacketParser.java 750418 2009-03-05 11:03:54Z vhennebert $ */

package org.apache.tika.parser.image.xmp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * This class is a parser for XMP packets. By default, it tries to locate the first XMP packet
 * it finds and parses it.
 * <p/>
 * Important: Before you use this class to look for an XMP packet in some random file, please read
 * the chapter on "Scanning Files for XMP Packets" in the XMP specification!
 * <p/>
 * Thic class was branched from http://xmlgraphics.apache.org/ XMPPacketParser.
 * See also org.semanticdesktop.aperture.extractor.xmp.XMPExtractor, a variant.
 * <p/>
 * PDFBox's XMPBox started requiring the xpacket begin to parse.
 * This differs from the original PacketScanner in that it captures the xpacket
 * stream begin and end
 */
public class NewXMPPacketScanner {

    private static final byte[] PACKET_HEADER;
    private static final byte[] PACKET_HEADER_END;
    private static final byte[] PACKET_TRAILER;
    private static final byte[] PACKET_TRAILER_END;

    private boolean inHit = false;//should we be capturing all bytes...are we in a hit?

    static {
        try {
            PACKET_HEADER = "<?xpacket begin=".getBytes("US-ASCII");
            PACKET_HEADER_END = "?>".getBytes("US-ASCII");
            PACKET_TRAILER = "<?xpacket".getBytes("US-ASCII");
            PACKET_TRAILER_END = "?>".getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Incompatible JVM! US-ASCII encoding not supported.");
        }
    }

    private boolean find(InputStream in, byte[] match, OutputStream out)
            throws IOException {
        int found = 0;
        int len = match.length;
        int b;
        boolean foundIt = false;
        while ((b = in.read()) >= 0) {
            if (inHit) {
                out.write(b);
            }
            if (b == match[found]) {
                found++;
                if (found == len) {
                    foundIt = true;
                    break;
                }
            } else {
                found = 0;
            }
        }
        if (foundIt && !inHit) {//if you haven't already recorded it
            out.write(match);
        }
        return foundIt;
    }

    /**
     * Locates an XMP packet in a stream, parses it and returns the XMP metadata. If no
     * XMP packet is found until the stream ends, null is returned. Note: This method
     * only finds the first XMP packet in a stream. And it cannot determine whether it
     * has found the right XMP packet if there are multiple packets.
     * <p/>
     * Does <em>not</em> close the stream.
     * If XMP block was found reading can continue below the block.
     *
     * @param in     the InputStream to search
     * @param xmlOut to write the XMP packet to
     * @return true if XMP packet is found, false otherwise
     * @throws IOException          if an I/O error occurs
     */
    public boolean parse(InputStream in, OutputStream xmlOut) throws IOException {
        inHit = false;//reset for consecutive parses

        boolean foundXMP = find(in, PACKET_HEADER, xmlOut);
        if (!foundXMP) {
            return false;
        }
        inHit = true;
        //TODO Inspect "begin" attribute!
        if (!find(in, PACKET_HEADER_END, xmlOut)) {
            throw new IOException("Invalid XMP packet header!");
        }
        //TODO Do with TeeInputStream when Commons IO 1.4 is available
        if (!find(in, PACKET_TRAILER, xmlOut)) {
            throw new IOException("XMP packet not properly terminated!");
        }
        if (!find(in, PACKET_TRAILER_END, xmlOut)) {
            throw new IOException("XMP packet trailer not properly terminated!");
        }
        return true;
    }

}

