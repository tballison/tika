package org.apache.tika.parser.pdf;


import static org.junit.Assert.assertEquals;

import javax.xml.transform.TransformerException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDMetadata;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType0Font;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.xmpbox.XMPMetadata;
import org.apache.xmpbox.schema.DublinCoreSchema;
import org.apache.xmpbox.xml.DomXmpParser;
import org.apache.xmpbox.xml.XmpSerializer;
import org.junit.Test;

public class PDFTestFileGenerator {

    @Test
    public void testGreekDetection() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(77);
        bos.write(105);
        bos.write(99);
        bos.write(114);
        bos.write(111);
        bos.write(32);
        bos.write(211);
        bos.write(245);
        bos.write(236);
        bos.write(236);
        bos.write(229);
        bos.write(244);
        bos.write(239);
        bos.write(247);
        bos.write(222);

        AutoDetectReader reader = new AutoDetectReader(new ByteArrayInputStream(bos.toByteArray()));
        assertEquals("ISO-8859-7", reader.getCharset());
    }

    public void createOctalEncodedMetadata(Path outputPath) throws IOException {
        PDDocument doc = new PDDocument();
        try {

            PDPage page = new PDPage();
            doc.addPage(page);
            PDFont font = PDType0Font.load(doc, this.getClass().getResourceAsStream("/test-documents/testTrueType3.ttf"));
            String message = "This file contains an octal encoded string in the title field of the xmp";
            // create a page with the message
            PDPageContentStream contents = new PDPageContentStream(doc, page);
            contents.beginText();
            contents.setFont(font, 12);
            contents.newLineAtOffset(100, 700);
            contents.showText(message);
            contents.endText();
            contents.saveGraphicsState();
            contents.close();

            // add XMP metadata
            XMPMetadata xmp = XMPMetadata.createXMPMetadata();

            try {

                    DublinCoreSchema dc = xmp.createAndAddDublinCoreSchema();
                    dc.setTitle("\\376\\377\\000M\\000i\\000c\\000r\\000o\\000s\\000o\\000f\\000t");
//                    dc.addTitle("zh", "普林斯顿大学");
                    //dc.setTitle("this is the title");
//                    dc.addTitle("el", "Microsoft Word - \\323\\365\\354\\354\\345\\364\\357\\367\\336 \\364\\347\\362 PRAKSIS \\363\\364\\357");

                    XmpSerializer serializer = new XmpSerializer();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    serializer.serialize(xmp, baos, true);
                    PDMetadata metadata = new PDMetadata(doc);
                    metadata.importXMPMetadata(baos.toByteArray());
                    doc.getDocumentCatalog().setMetadata(metadata);
            } catch (TransformerException e) {
                e.printStackTrace();
            }
            doc.save(outputPath.toFile());
        } finally {
            doc.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(new File(".").getAbsolutePath());
        Path outputPath = Paths.get("C:\\Users\\tallison\\Desktop\\tmp\\tika-problems\\testPDF_OctalEncodedXMP.pdf");
        PDFTestFileGenerator g = new PDFTestFileGenerator();
        g.createOctalEncodedMetadata(outputPath);
        PDDocument doc = null;
        try {
            doc = PDDocument.load(outputPath.toFile());
            InputStream xmpStream = doc.getDocumentCatalog().getMetadata().exportXMPMetadata();
            DomXmpParser xmpParser = new DomXmpParser();
            xmpParser.setStrictParsing(false);
            XMPMetadata xmp = xmpParser.parse(xmpStream);
            DublinCoreSchema dublinCoreSchema = xmp.getDublinCoreSchema();
            System.out.println(dublinCoreSchema.getTitle());
            System.out.println(dublinCoreSchema.getTitle("zh"));
        } finally {
            doc.close();
        }
    }
}
