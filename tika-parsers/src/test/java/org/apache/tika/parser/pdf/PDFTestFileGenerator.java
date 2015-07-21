package org.apache.tika.parser.pdf;


import javax.xml.transform.TransformerException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.drew.lang.StringUtil;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDMetadata;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType0Font;
import org.apache.xmpbox.XMPMetadata;
import org.apache.xmpbox.schema.DublinCoreSchema;
import org.apache.xmpbox.xml.XmpSerializer;

public class PDFTestFileGenerator {
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
    //                dc.addTitle("en", "\\376\\377\\000M\\000i\\000c\\000r\\000o\\000s\\000o\\000f\\000t");
    //                dc.addTitle("zh", "普林斯顿大学");
                    dc.setTitle("this is the title");

                    XmpSerializer serializer = new XmpSerializer();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    serializer.serialize(xmp, baos, true);
                    System.out.println(StringUtil.fromStream(new ByteArrayInputStream(baos.toByteArray())));
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
        Path outputPath = Paths.get("C:\\Users\\tallison\\Desktop\\tmp\\tika-problems\\testPDF_OctalEncodedXMP.pdf");
        PDFTestFileGenerator g = new PDFTestFileGenerator();
        g.createOctalEncodedMetadata(outputPath);
    }
}
