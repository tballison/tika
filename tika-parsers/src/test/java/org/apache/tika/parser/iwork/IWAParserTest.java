package org.apache.tika.parser.iwork;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.iwork.proto.TSPArchiveMessages;
import org.junit.Test;

import javax.print.DocFlavor;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.bouncycastle.crypto.tls.CipherType.stream;

/**
 * Created by TALLISON on 5/26/2016.
 */
public class IWAParserTest extends TikaTest {
    @Test
    public void testFirstSteps() throws Exception {
        System.out.println("hello");
//        XMLResult r = getXML("snappyIWATest.raw", new IWAParser(), new Metadata());
        //getResourceAsStream("/test-documents/snappyIWATest2.raw");
        InputStream is =
                Files.newInputStream(Paths.get("C:\\data\\commons_uncompressed.iwa"));


        TSPArchiveMessages.ArchiveInfo ai = TSPArchiveMessages.ArchiveInfo.parseDelimitedFrom(is);
        for (TSPArchiveMessages.MessageInfo messageInfo : ai.getMessageInfosList()) {
            System.out.println("MI: " + messageInfo.getType());
            System.out.println("data ref count:" + messageInfo.getDataReferencesCount());
            messageInfo.get
            for (TSPArchiveMessages.FieldInfo fi : messageInfo.getFieldInfosList()) {
                System.out.println("FI: " + fi.getType());
            }
        }
        System.out.println("goodbye");
        //JsonFormat.printer().print((Message)parser.parseFrom(is));
    }

    @Test
    public void dumpStream() throws Exception {
        Path out = Paths.get("C:\\data\\snappy_uncompressed.iwa");
        Files.copy(new SnappyNoCRCFramedInputStream(getResourceAsStream("/test-documents/unsnapped.iwa")),
                out);
    }
}
