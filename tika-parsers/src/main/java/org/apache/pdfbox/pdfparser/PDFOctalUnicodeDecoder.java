/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pdfbox.pdfparser;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.pdfbox.cos.COSString;

/**
 * In fairly rare cases, a PDF's XMP will contain a string that
 * has incorrectly been encoded with PDFEncoding: an octal for non-ascii and
 * ascii for ascii, e.g. "\376\377\000M\000i\000c\000r\000o\000s\000o\000f\000t\000"
 * <p>
 * This class can be used to decode those strings.
 * <p>
 * See TIKA-1678.  Many thanks to Andrew Jackson for raising this issue
 * and Tilman Hausherr for the solution.
 * <p>
 * Unfortunately because {@link BaseParser#parseCOSString()} is protected, we
 * had to put this in o.a.pdfbox.pdfparser and not in o.a.t.parser.pdf
 */
public class PDFOctalUnicodeDecoder {

    private static final String[] PDF_ENCODING_BOMS = {
            "\\376\\377", //UTF-16BE
            "\\377\\376", //UTF-16LE
            "\\357\\273\\277"//UTF-8
    };

    /**
     * Does this string contain an octal-encoded UTF BOM?
     * Call this statically to determine if you should then parse it.
     * @param s
     * @return
     */
    public static boolean shouldDecode(String s) {
        if (s == null || s.length() < 8) {
            return false;
        }
        for (String BOM : PDF_ENCODING_BOMS) {
            if (s.startsWith(BOM)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This assumes that {@link #shouldDecode(String)} has been called
     * and has returned true.  If you run this on a non-octal encoded string,
     * disaster will happen!
     *
     * @param value
     * @return
     */
    public String decode(String value) {
        try {
            byte[] bytes = new String("(" + value + ")").getBytes("ISO-8859-1");
            SequentialSource source = new InputStreamSource(new ByteArrayInputStream(bytes));
            COSStringParser p = new COSStringParser(source);
            COSString cosString = p.parseCOSString();
            if (cosString != null) {
                return cosString.getString();
            }
        } catch (IOException e) {
            //oh well, we tried.
        }
        //just return value if something went wrong
        return value;
    }

    private class COSStringParser extends BaseParser {

        private COSStringParser(SequentialSource buffer) throws IOException {
            super(buffer);
        }
    }
}
