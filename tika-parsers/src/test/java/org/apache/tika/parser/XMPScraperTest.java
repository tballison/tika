package org.apache.tika.parser;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.tika.io.IOUtils;
import org.apache.tika.parser.xmp.XMPPacket;
import org.apache.tika.parser.xmp.XMPScraper;
import org.junit.Test;

/**
 * Created by tallison on 12/15/2015.
 */
public class XMPScraperTest {

    @Test
    public void extractXMP() throws Exception {
        File dir = new File(this.getClass().getResource("/test-documents").toURI());
        for (File f : dir.listFiles()) {
//            if (! f.getName().contains("sampleAcrobat_4_x.pdf")) {
            if (! f.getName().contains("testPDF_Version.4.x.pdf")) {
    //            continue;
            }
            if (f.isDirectory()) {
                continue;
            }
            try (InputStream is = new BufferedInputStream(new FileInputStream(f))) {
                XMPScraper scraper = new XMPScraper();
                List<XMPPacket> packets = scraper.scrape(is);
                if (packets.size() > 1) {
                    for (int i = 0; i < packets.size(); i++) {
                        XMPPacket p = packets.get(i);
                        System.out.println("header:"+ IOUtils.toString(p.getHeader(),
                                StandardCharsets.US_ASCII.name()));
                        System.out.println("payload:"+ IOUtils.toString(p.getPayload(),
                                StandardCharsets.US_ASCII.name()));
                    }
                    System.out.println(f + " : " + packets.size());
                }
            }
        }
    }
}
