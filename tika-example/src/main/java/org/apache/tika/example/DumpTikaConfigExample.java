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

package org.apache.tika.example;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.tika.config.LoadErrorHandler;
import org.apache.tika.config.ServiceLoader;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.CompositeDetector;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.language.translate.DefaultTranslator;
import org.apache.tika.language.translate.Translator;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;


/**
 * This class shows how to dump a TikaConfig object to a configuration file.
 * This allows users to easily dump the default TikaConfig as a base from which
 * to start if they want to modify the default configuration file.
 * <p>
 * For those who want to modify the mimes file, take a look at
 * tika-core/src/main/resources/org/apache/tika/mime/tika-mimetypes.xml
 * for inspiration.  Consider adding org/apache/tika/mime/custom-mimetypes.xml
 * for your custom mime types.
 */
public class DumpTikaConfigExample {
    /**
     * @param config config file to dump
     * @param writer writer to which to write
     * @throws Exception
     */
    public void dump(TikaConfig config, Mode mode, Writer writer, String encoding) throws Exception {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        
        // root elements
        Document doc = docBuilder.newDocument();
        Element rootElement = doc.createElement("properties");

        doc.appendChild(rootElement);
        addMimeComment(mode, rootElement, doc);
        addServiceLoader(mode, rootElement, doc, config);
        addTranslator(mode, rootElement, doc, config);
        addDetectors(mode, rootElement, doc, config);
        addParsers(mode, rootElement, doc, config);
        // TODO Service Loader section

        // now write
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.ENCODING, encoding);
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(writer);

        transformer.transform(source, result);
    }

    private void addServiceLoader(Mode mode, Element rootElement, Document doc, TikaConfig config) {
        ServiceLoader loader = config.getServiceLoader();
        
        if (mode == Mode.MINIMAL) {
            // Is this the default?
            if (loader.isDynamic() && loader.getLoadErrorHandler() == LoadErrorHandler.IGNORE) {
                // Default config, no need to output anything
                return;
            }
        }
        
        Element dslEl = doc.createElement("service-loader");
        dslEl.setAttribute("dynamic", Boolean.toString(loader.isDynamic()));
        dslEl.setAttribute("loadErrorHandler", loader.getLoadErrorHandler().toString());
        rootElement.appendChild(dslEl);
    }
    
    private void addTranslator(Mode mode, Element rootElement, Document doc, TikaConfig config) {
        // Unlike the other entries, TikaConfig only wants one of
        //  these, and no outer <translators> list
        Translator translator = config.getTranslator();
        if (mode == Mode.MINIMAL && translator instanceof DefaultTranslator) {
            Node mimeComment = doc.createComment(
                    "for example: <translator class=\"org.apache.tika.language.translate.GoogleTranslator\"/>");
            rootElement.appendChild(mimeComment);
        } else {
            if (translator instanceof DefaultTranslator && mode == Mode.STATIC) {
                translator = ((DefaultTranslator)translator).getTranslator();
            }
            if (translator != null) {
                Element translatorElement = doc.createElement("translator");
                translatorElement.setAttribute("class", translator.getClass().getCanonicalName());
                rootElement.appendChild(translatorElement);
            } else {
                rootElement.appendChild(doc.createComment("No translators available"));
            }
        }
    }

    private void addMimeComment(Mode mode, Element rootElement, Document doc) {
        Node mimeComment = doc.createComment(
                "for example: <mimeTypeRepository resource=\"/org/apache/tika/mime/tika-mimetypes.xml\"/>");
        rootElement.appendChild(mimeComment);
    }

    private void addDetectors(Mode mode, Element rootElement, Document doc, TikaConfig config) throws Exception {
        Detector detector = config.getDetector();
        
        if (mode == Mode.MINIMAL && detector instanceof DefaultDetector) {
            // Don't output anything, all using defaults
            Node detComment = doc.createComment(
                    "for example: <detectors><detector class=\"org.apache.tika.detector.MimeTypes\"></detectors>");
            rootElement.appendChild(detComment);
            return;
        }
        
        Element detectorsElement = doc.createElement("detectors");
        if (mode == Mode.CURRENT && detector instanceof DefaultDetector ||
            ! (detector instanceof CompositeDetector)) {
            Element detectorElement = doc.createElement("detector");
            detectorElement.setAttribute("class", detector.getClass().getCanonicalName());
            detectorsElement.appendChild(detectorElement);
        } else {
            List<Detector> children = ((CompositeDetector)detector).getDetectors();
            for (Detector d : children) {
                Element detectorElement = doc.createElement("detector");
                detectorElement.setAttribute("class", d.getClass().getCanonicalName());
                detectorsElement.appendChild(detectorElement);
            }
        }
        rootElement.appendChild(detectorsElement);
    }

    private void addParsers(Mode mode, Element rootElement, Document doc, TikaConfig config) throws Exception {
        Parser parser = config.getParser();
        if (mode == Mode.MINIMAL && parser instanceof DefaultParser) {
            // Don't output anything, all using defaults
            return;
        } else if (mode == Mode.MINIMAL) {
            mode = Mode.CURRENT;
        }

        Element parsersElement = doc.createElement("parsers");
        rootElement.appendChild(parsersElement);
        
        addParser(mode, parsersElement, doc, parser);
    }
    private void addParser(Mode mode, Element rootElement, Document doc, Parser parser) throws Exception {
        // If the parser is decorated, is it a kind where we output the parser inside?
        ParserDecorator decoration = null;
        if (parser instanceof ParserDecorator) {
            if (parser.getClass().getName().startsWith(ParserDecorator.class.getName()+"$")) {
                decoration = ((ParserDecorator)parser);
                parser = decoration.getWrappedParser();
            }
        }
        
        boolean outputParser = true;
        List<Parser> children = Collections.emptyList();
        if (mode == Mode.CURRENT && parser instanceof DefaultParser) {
            // Only output the parser, not the children
        } else if (parser instanceof CompositeParser) {
            children = ((CompositeParser)parser).getAllComponentParsers();
            // Special case for a naked composite
            if (parser.getClass().equals(CompositeParser.class)) {
                outputParser = false;
            }
            // Special case for making Default to static
            if (mode == Mode.STATIC && parser instanceof DefaultParser) {
                outputParser = false;
            }
        }
        
        if (outputParser) {
            rootElement = addParser(rootElement, doc, parser, decoration);
        }
        for (Parser childParser : children) {
            addParser(mode, rootElement, doc, childParser);
        }
        // TODO Parser Exclusions
    }
    private Element addParser(Element rootElement, Document doc, Parser parser, ParserDecorator decorator) throws Exception {
        ParseContext context = new ParseContext();
        
        Set<MediaType> addedTypes = new TreeSet<>();
        Set<MediaType> excludedTypes = new TreeSet<>();
        if (decorator != null) {
            Set<MediaType> types = new TreeSet<>();
            types.addAll(decorator.getSupportedTypes(context));
            addedTypes.addAll(types);
            
            for (MediaType type : parser.getSupportedTypes(context)) {
                if (! types.contains(type)) {
                    excludedTypes.add(type);
                }
                addedTypes.remove(type);
            }
        }
        
        String className = parser.getClass().getCanonicalName();
        Element parserElement = doc.createElement("parser");
        parserElement.setAttribute("class", className);
        rootElement.appendChild(parserElement);
        
        for (MediaType type : addedTypes) {
            Element mimeElement = doc.createElement("mime");
            mimeElement.appendChild(doc.createTextNode(type.toString()));
            parserElement.appendChild(mimeElement);
        }
        for (MediaType type : excludedTypes) {
            Element mimeElement = doc.createElement("mime-exclude");
            mimeElement.appendChild(doc.createTextNode(type.toString()));
            parserElement.appendChild(mimeElement);
        }
        
        return parserElement;
    }
    
    /**
     * @param args outputFile, outputEncoding, if args is empty, this prints to console
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Charset encoding = UTF_8;
        Mode mode = Mode.CURRENT;
        String filename = null;
        
        for (String arg : args) {
            if (arg.startsWith("-")) {
                if (arg.contains("-dump-minimal")) {
                    mode = Mode.MINIMAL;
                } else if (arg.contains("-dump-current")) {
                    mode = Mode.CURRENT;
                } else if (arg.contains("-dump-static")) {
                    mode = Mode.STATIC;
                } else {
                    System.out.println("Use:");
                    System.out.println("  DumpTikaConfig [--dump-minimal] [--dump-current] [--dump-static] [filename] [encoding]");
                    System.out.println("");
                    System.out.println("--dump-minimal    Produce the minimal config file");
                    System.out.println("--dump-current    The current (with defaults) config file");
                    System.out.println("--dump-static     Convert dynamic parts to static");
                    return;
                }
            } else if (filename == null) {
                filename = arg;
            } else {
                encoding = Charset.forName(arg);
            }
        }
        
        Writer writer = null;
        if (filename != null) {
            writer = new OutputStreamWriter(new FileOutputStream(filename), encoding);
        } else {
            writer = new StringWriter();
        }
        
        DumpTikaConfigExample ex = new DumpTikaConfigExample();
        ex.dump(TikaConfig.getDefaultConfig(), mode, writer, encoding.name());

        writer.flush();

        if (writer instanceof StringWriter) {
            System.out.println(writer.toString());
        }
        writer.close();
    }
    protected enum Mode {
        MINIMAL, CURRENT, STATIC;
    }
}
