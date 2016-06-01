package org.apache.tika.parser.iwork;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.iwork.util.JsonFormat;
import org.junit.Test;

import javax.print.DocFlavor;

import java.io.InputStream;

import static org.bouncycastle.crypto.tls.CipherType.stream;

/**
 * Created by TALLISON on 5/26/2016.
 */
public class IWAParserTest extends TikaTest {
    @Test
    public void testFirstSteps() throws Exception {
        Parser parser = new UnknownFieldSet.Parser();

//        XMLResult r = getXML("snappyIWATest.raw", new IWAParser(), new Metadata());
        InputStream is = getResourceAsStream("/test-documents/snappyIWATest2.raw");
        parser.parseFrom(is);
        JsonFormat.printer().print((Message)parser.parseFrom(is));
    }
}
