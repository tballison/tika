package org.apache.tika.parser.iwork;


import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Extension;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import static org.bouncycastle.asn1.x500.style.RFC4519Style.l;

public class IWAParser extends AbstractParser {

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return null;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
        Parser parser = new UnknownFieldSet.Parser();


 /*       Object parsed = parser.parseDelimitedFrom(stream);

        UnknownFieldSet unknownFieldSet = (UnknownFieldSet)parsed;
        for (Map.Entry<Integer, UnknownFieldSet.Field> e : unknownFieldSet.asMap().entrySet()) {
            UnknownFieldSet.Field field = e.getValue();

            System.out.println(e.getKey() + " : "+e.getValue());
            System.out.println("FIXED 32 size: " + field.getFixed32List().size());
            System.out.println("FIXED 64 size: " + field.getFixed64List().size());
            System.out.println("OTHER: "+field.getGroupList().size());
            System.out.println("VAR INT SIZE:"+field.getVarintList().size());
            System.out.println("VAR INT SIZE2:"+field.getLengthDelimitedList().size());

            for (Long lng : field.getVarintList()) {
                System.out.println("LONG: "+lng);
            }
            for (UnknownFieldSet set : field.getGroupList()) {
                System.out.println(set.toString());
            }
        }
        System.out.println("DONE");*/
        CodedInputStream codedInputStream = CodedInputStream.newInstance(stream);
        int sum = 0;
        while (true) {
            int len = codedInputStream.readRawVarint32();
            System.out.println(len);
            byte[] bytes = codedInputStream.readRawBytes(len);
            System.out.println(new String(bytes));
            sum += len;
            System.out.println("SUM: " + sum);
        }

    }
}
