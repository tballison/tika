package org.apache.tika;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Created by TALLISON on 7/27/2015.
 */
public class SimpleDirCrawler {
    private static File dir = new File("C:\\Users\\tallison\\Documents\\My Projects\\HSARPA\\Rhapsode\\input");

    Parser p = new AutoDetectParser();

    public void execute() {
        procDir(dir);
    }

    private void procDir(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                procDir(f);
            } else {
                procFile(f);
            }
        }
    }

    private void procFile(File f) {
        Metadata m = new Metadata();
        try (TikaInputStream tis = TikaInputStream.get(f, m)){
            ParseContext c = new ParseContext();
            c.set(Parser.class, p);
            ContentHandler h = new BodyContentHandler(-1);
            System.out.println("about to parse:"+f.getAbsolutePath());
            p.parse(tis, h, m, c);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void runDir() {
        new SimpleDirCrawler().execute();
    }
}
