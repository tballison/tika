package org.apache.tika.parser;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.adobe.xmp.XMPMeta;
import com.adobe.xmp.XMPMetaFactory;
import com.adobe.xmp.options.ParseOptions;
import org.apache.tika.TikaTest;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.xmp.XMPPacket;
import org.apache.tika.parser.xmp.XMPScraper;
import org.junit.Test;

/**
 * Created by tallison on 12/15/2015.
 */
public class XMPScraperTest extends TikaTest {

    @Test
    public void extractXMP() throws Exception {
        File dir = new File(this.getClass().getResource("/test-documents").toURI());
        for (File f : dir.listFiles()) {
//            if (! f.getName().contains("sampleAcrobat_4_x.pdf")) {
            if (! f.getName().contains("Noakes")) {
               continue;
            }
            XMLResult r = getXML("withXMP_fromNoakes.jpg");
            Metadata m = r.metadata;
            for (String n : m.names()) {
                for (String v : m.getValues(n)) {
                    System.out.println(n + " : " + v);
                }
            }
            if (f.isDirectory()) {
                continue;
            }
            System.out.println(f.getName());
            try (InputStream is = new BufferedInputStream(new FileInputStream(f))) {
                XMPScraper scraper = new XMPScraper();
                List<XMPPacket> packets = scraper.scrape(is);
                if (packets.size() > 0) {
                    for (int i = 0; i < packets.size(); i++) {
                        XMPPacket p = packets.get(i);
                        System.out.println("header:"+ IOUtils.toString(p.getHeader(),
                                StandardCharsets.US_ASCII.name()));
                        System.out.println("payload:"+ IOUtils.toString(p.getPayload(),
                                StandardCharsets.US_ASCII.name()));
                        ParseOptions parseOptions = new ParseOptions();
                        parseOptions.setFixControlChars(true);
                        parseOptions.setStrictAliasing(false);
                        parseOptions.setAcceptLatin1(true);
                        parseOptions.setOmitNormalization(false);
                        parseOptions.setRequireXMPMeta(false);
                        XMPMeta xm = XMPMetaFactory.parse(p.getFullPacket(), parseOptions);

                    }
                    System.out.println(f + " : " + packets.size());
                }
            }
        }
    }
}
