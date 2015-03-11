package org.apache.tika.util;

import java.io.File;
import java.util.Vector;

import org.apache.log4j.receivers.xml.XMLDecoder;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Created by TALLISON on 3/9/2015.
 */
public class TestReadLog {

    public static void main(String[] args) throws Exception {
        File f = new File("C:\\Users\\tallison\\Desktop\\tmp\\timeout-copy.log");
        XMLDecoder decoder = new XMLDecoder();
        Vector<LoggingEvent> v = decoder.decode(f.toURI().toURL());
        for (int i = 0; i < v.size(); i++) {
            System.out.println(v.get(i).getMessage());
        }
    }
}
