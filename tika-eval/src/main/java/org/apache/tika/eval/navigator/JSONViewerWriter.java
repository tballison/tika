package org.apache.tika.eval.navigator;

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

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.PrettyMetadataKeyComparator;
import org.apache.tika.parser.RecursiveParserWrapper;

@Provider
@Produces(MediaType.TEXT_HTML)
public class JSONViewerWriter implements MessageBodyWriter<List<Metadata>> {

    private final static Pattern NEW_LINES_PATTERN = Pattern.compile("(?: *(?:\r\n|[\r\n]) *){3,}");

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        boolean isWriteable = false;
        if (List.class.isAssignableFrom(type) && genericType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) genericType;
            Type[] actualTypeArgs = (parameterizedType.getActualTypeArguments());
            isWriteable = (actualTypeArgs.length == 1 &&
                    actualTypeArgs[0].equals(Metadata.class));
        } else {
            isWriteable = false;
        }
        return isWriteable;
    }

    public long getSize(List<Metadata> data, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(List<Metadata> metadata, Class<?> type, Type genericType, Annotation[] annotations,
                        MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException,
            WebApplicationException {
        //TODO: switch to templates/velocity asap!!!
        Writer writer = new OutputStreamWriter(entityStream, "UTF-8");
        writer.write("<html>");
        writer.write("<head>");
        writer.write("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">");
        writer.write("</head>");

        writer.write("<body>");
        writer.write("<table border=\"2\">");
        int i = 0;
        for (Metadata m : metadata) {
            String[] names = m.names();
            if (names == null) {
                continue;
            }
            Arrays.sort(names, new PrettyMetadataKeyComparator());
            for (String n : names) {
                for (String v : m.getValues(n)) {
                    writer.write("<tr>");
                    writer.write("<td>");
                    writer.write(StringEscapeUtils.escapeHtml4(n));
                    writer.write("</td>");
                    writer.write("<td>");
                    //treat the content specially, compress multiple new lines and trim
                    if (n.equals(RecursiveParserWrapper.TIKA_CONTENT.getName())) {
                        writer.write("<pre>");
                        String esc = StringEscapeUtils.escapeHtml4(v);
                        Matcher matcher = NEW_LINES_PATTERN.matcher(esc);
                        esc = matcher.replaceAll("\n\n");
                        esc = esc.trim();
                        writer.write(esc);
                        writer.write("</pre>");
                    } else {
                        writer.write(StringEscapeUtils.escapeHtml4(v));
                    }
                    writer.write("</td>");
                    writer.write("</tr>");
                }
            }
            if (i > 0 && i < metadata.size()) {
                writer.write("<tr/>");
            }
            i++;
        }
        writer.write("</table>");
        writer.write("</body>");
        writer.write("</html>");
        writer.flush();
        entityStream.flush();
    }
}

