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
package org.apache.tika.parser.pdf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSString;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdfparser.PDFOctalUnicodeDecoder;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.AccessPermissions;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.utils.ExceptionUtils;
import org.apache.xmpbox.XMPMetadata;
import org.apache.xmpbox.schema.DublinCoreSchema;
import org.apache.xmpbox.schema.PDFAIdentificationSchema;
import org.apache.xmpbox.xml.DomXmpParser;
import org.apache.xmpbox.xml.XmpParsingException;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * PDF parser.
 * <p/>
 * This parser can process also encrypted PDF documents if the required
 * password is given as a part of the input metadata associated with a
 * document. If no password is given, then this parser will try decrypting
 * the document using the empty password that's often used with PDFs. If
 * the PDF contains any embedded documents (for example as part of a PDF
 * package) then this parser will use the {@link EmbeddedDocumentExtractor}
 * to handle them.
 * <p/>
 * As of Tika 1.6, it is possible to extract inline images with
 * the {@link EmbeddedDocumentExtractor} as if they were regular
 * attachments.  By default, this feature is turned off because of
 * the potentially enormous number and size of inline images.  To
 * turn this feature on, see
 * {@link PDFParserConfig#setExtractInlineImages(boolean)}.
 */
public class PDFParser extends AbstractParser {


    /**
     * Metadata key for giving the document password to the parser.
     *
     * @since Apache Tika 0.5
     * @deprecated Supply a {@link PasswordProvider} on the {@link ParseContext} instead
     */
    public static final String PASSWORD = "org.apache.tika.parser.pdf.password";
    private static final MediaType MEDIA_TYPE = MediaType.application("pdf");



    /**
     * Serial version UID
     */
    private static final long serialVersionUID = -752276948656079347L;
    private static final long MAX_MEMORY_BUFFER_BYTES = 10L*1024L*1024L;//10 MB
    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MEDIA_TYPE);
    private PDFParserConfig defaultConfig = new PDFParserConfig();

    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    public void parse(
            InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {

        PDDocument pdfDocument = null;
        //config from context, or default if not set via context
        PDFParserConfig localConfig = context.get(PDFParserConfig.class, defaultConfig);
        String password = getPassword(metadata, context);
        MemoryUsageSetting memoryUsageSetting = MemoryUsageSetting.setupMixed(MAX_MEMORY_BUFFER_BYTES);
        try {
            // PDFBox can process entirely in memory, or can use scratch files
            TikaInputStream tstream = TikaInputStream.cast(stream);
            if (tstream != null && tstream.hasFile()) {
                pdfDocument = PDDocument.load(tstream.getFile(), password, memoryUsageSetting);
            } else {
                // Go for the normal, stream based in-memory parsing
                pdfDocument = PDDocument.load(new CloseShieldInputStream(stream), password, memoryUsageSetting);
            }
            metadata.set("pdf:encrypted", Boolean.toString(pdfDocument.isEncrypted()));

            metadata.set(Metadata.CONTENT_TYPE, "application/pdf");
            extractMetadata(pdfDocument, metadata);

            AccessChecker checker = localConfig.getAccessChecker();
            checker.check(metadata);
            if (handler != null) {
                PDF2XHTML.process(pdfDocument, handler, context, metadata, localConfig);
            }

        } catch (InvalidPasswordException e) {
            metadata.set("pdf:encrypted", Boolean.toString(true));

            throw new EncryptedDocumentException(e);
        } catch (IOException e) {
            if (e.getMessage() != null &&
                    e.getMessage().contains("Error (CryptographyException)")) {
                metadata.set("pdf:encrypted", Boolean.toString(true));
                throw new EncryptedDocumentException(e);
            }
            //rethrow any other IOExceptions
            throw e;
        } finally {
            if (pdfDocument != null) {
                pdfDocument.close();
            }
        }
    }

    private String getPassword(Metadata metadata, ParseContext context) {
        String password = null;

        // Did they supply a new style Password Provider?
        PasswordProvider passwordProvider = context.get(PasswordProvider.class);
        if (passwordProvider != null) {
            password = passwordProvider.getPassword(metadata);
        }

        // Fall back on the old style metadata if set
        if (password == null && metadata.get(PASSWORD) != null) {
            password = metadata.get(PASSWORD);
        }

        // If no password is given, use an empty string as the default
        if (password == null) {
            password = "";
        }
        return password;
    }


    private void extractMetadata(PDDocument document, Metadata metadata)
            throws TikaException {

        //first extract AccessPermissions
        AccessPermission ap = document.getCurrentAccessPermission();
        metadata.set(AccessPermissions.EXTRACT_FOR_ACCESSIBILITY,
                Boolean.toString(ap.canExtractForAccessibility()));
        metadata.set(AccessPermissions.EXTRACT_CONTENT,
                Boolean.toString(ap.canExtractContent()));
        metadata.set(AccessPermissions.ASSEMBLE_DOCUMENT,
                Boolean.toString(ap.canAssembleDocument()));
        metadata.set(AccessPermissions.FILL_IN_FORM,
                Boolean.toString(ap.canFillInForm()));
        metadata.set(AccessPermissions.CAN_MODIFY,
                Boolean.toString(ap.canModify()));
        metadata.set(AccessPermissions.CAN_MODIFY_ANNOTATIONS,
                Boolean.toString(ap.canModifyAnnotations()));
        metadata.set(AccessPermissions.CAN_PRINT,
                Boolean.toString(ap.canPrint()));
        metadata.set(AccessPermissions.CAN_PRINT_DEGRADED,
                Boolean.toString(ap.canPrintDegraded()));

        //now go for the XMP stuff
        XMPMetadata xmp = null;
        DublinCoreSchema dcSchema = null;
        try {
            DomXmpParser xmpParser = new DomXmpParser();
            xmpParser.setStrictParsing(false);

            if (document.getDocumentCatalog().getMetadata() != null) {
                xmp = xmpParser.parse(document.getDocumentCatalog().getMetadata().exportXMPMetadata());
            }
            if (xmp != null) {
                dcSchema = xmp.getDublinCoreSchema();
            }
        } catch (IOException|XmpParsingException e) {
            e.printStackTrace();
            metadata.set(TikaCoreProperties.TIKA_META_PREFIX + "pdf:metadata-xmp-parse-failed", ExceptionUtils.getStackTrace(e));
        }

        PDDocumentInformation info = document.getDocumentInformation();
        metadata.set(PagedText.N_PAGES, document.getNumberOfPages());
        addMetadata(metadata, TikaCoreProperties.CREATOR_TOOL, info.getCreator());
        addMetadata(metadata, TikaCoreProperties.KEYWORDS, info.getKeywords());
        addMetadata(metadata, "producer", info.getProducer());

        if (dcSchema != null) {
            addMetadata(metadata, TikaCoreProperties.CREATOR, dcSchema.getCreators(), info.getAuthor());
            addMetadata(metadata, TikaCoreProperties.CONTRIBUTOR, dcSchema.getContributors(), (String) null);
            addMetadata(metadata, TikaCoreProperties.COVERAGE, dcSchema.getCoverage());
            addMetadata(metadata, TikaCoreProperties.SOURCE, dcSchema.getSource());
            addMetadata(metadata, TikaCoreProperties.PUBLISHER, dcSchema.getPublishers(), (String)null);
            addMetadata(metadata, TikaCoreProperties.FORMAT, dcSchema.getFormat());
            //TODO -- need to extract multiple langs for description, title, rights!!!
            addMetadata(metadata, TikaCoreProperties.DESCRIPTION, dcSchema.getDescription(), (String) null);
            System.out.println("DC TITLE: "+dcSchema.getTitle());
            addMetadata(metadata, TikaCoreProperties.TITLE, dcSchema.getTitle(), info.getTitle());
            addMetadata(metadata, TikaCoreProperties.RIGHTS, dcSchema.getRights());
        } else {
            addMetadata(metadata, TikaCoreProperties.CREATOR, info.getAuthor());
            addMetadata(metadata, TikaCoreProperties.TITLE, info.getTitle());
        }
        // TODO: Move to description in Tika 2.0
        addMetadata(metadata, TikaCoreProperties.TRANSITION_SUBJECT_TO_OO_SUBJECT, info.getSubject());
        addMetadata(metadata, "trapped", info.getTrapped());
        // TODO Remove these in Tika 2.0
        addMetadata(metadata, "created", info.getCreationDate());
        addMetadata(metadata, TikaCoreProperties.CREATED, info.getCreationDate());

        Calendar modified = info.getModificationDate();
        addMetadata(metadata, Metadata.LAST_MODIFIED, modified);
        addMetadata(metadata, TikaCoreProperties.MODIFIED, modified);


        // All remaining metadata is custom
        // Copy this over as-is
        List<String> handledMetadata = Arrays.asList("Author", "Creator", "CreationDate", "ModDate",
                "Keywords", "Producer", "Subject", "Title", "Trapped");
        for (COSName key : info.getCOSObject().keySet()) {
            String name = key.getName();
            if (!handledMetadata.contains(name)) {
                addMetadata(metadata, name, info.getCOSObject().getDictionaryObject(key));
            }
        }

        //try to get the various versions
        //Caveats:
        //    there is currently a fair amount of redundancy
        //    TikaCoreProperties.FORMAT can be multivalued
        //    There are also three potential pdf specific version keys: pdf:PDFVersion, pdfa:PDFVersion, pdf:PDFExtensionVersion        
        metadata.set("pdf:PDFVersion", Float.toString(document.getDocument().getVersion()));
        metadata.add(TikaCoreProperties.FORMAT.getName(),
                MEDIA_TYPE.toString() + "; version=" +
                        Float.toString(document.getDocument().getVersion()));

        if (xmp != null) {
            PDFAIdentificationSchema pdfaidxmp = new PDFAIdentificationSchema(xmp);
            if (pdfaidxmp != null) {
                if (pdfaidxmp.getPart() != null) {
                    metadata.set("pdfaid:part", Integer.toString(pdfaidxmp.getPart()));
                }
                if (pdfaidxmp.getConformance() != null) {
                    metadata.set("pdfaid:conformance", pdfaidxmp.getConformance());
                    String version = "A-" + pdfaidxmp.getPart() + pdfaidxmp.getConformance().toLowerCase(Locale.ROOT);
                    metadata.set("pdfa:PDFVersion", version);
                    metadata.add(TikaCoreProperties.FORMAT.getName(),
                            MEDIA_TYPE.toString() + "; version=\"" + version + "\"");
                }
            }
            // TODO WARN if this XMP version is inconsistent with document header version?
        }

        //TODO: Let's try to move this into PDFBox.
        //or can we get this from the PDFAExtensionSchema
        //Attempt to determine Adobe extension level, if present:
        COSDictionary root = document.getDocumentCatalog().getCOSObject();
        COSDictionary extensions = (COSDictionary) root.getDictionaryObject(COSName.getPDFName("Extensions"));
        if (extensions != null) {
            for (COSName extName : extensions.keySet()) {
                // If it's an Adobe one, interpret it to determine the extension level:
                if (extName.equals(COSName.getPDFName("ADBE"))) {
                    COSDictionary adobeExt = (COSDictionary) extensions.getDictionaryObject(extName);
                    if (adobeExt != null) {
                        String baseVersion = adobeExt.getNameAsString(COSName.getPDFName("BaseVersion"));
                        int el = adobeExt.getInt(COSName.getPDFName("ExtensionLevel"));
                        //-1 is sentinel value that something went wrong in getInt
                        if (el != -1) {
                            metadata.set("pdf:PDFExtensionVersion", baseVersion + " Adobe Extension Level " + el);
                            metadata.add(TikaCoreProperties.FORMAT.getName(),
                                    MEDIA_TYPE.toString() + "; version=\"" + baseVersion + " Adobe Extension Level " + el + "\"");
                        }
                    }
                } else {
                    // WARN that there is an Extension, but it's not Adobe's, and so is a 'new' format'.
                    metadata.set("pdf:foundNonAdobeExtensionName", extName.getName());
                }
            }
        }
    }
    private void addMetadata(Metadata metadata, Property property, String preferredValue, String backupValue) {
        if (preferredValue == null || preferredValue.trim().length() == 0) {
            addMetadata(metadata, property, backupValue);
        } else {
            addMetadata(metadata, property, preferredValue);
        }
    }

    private void addMetadata(Metadata metadata, Property property,
                             List<String> preferredValues, String backupValue) {
        if (preferredValues == null || preferredValues.size() == 0) {
            addMetadata(metadata, property, backupValue);
            return;
        }
        if (property.isMultiValuePermitted()) {
            for (String v : preferredValues) {
                addMetadata(metadata, property, v);
            }
        } else {
            //just take the first
            String v = preferredValues.get(0);
            if (v != null) {
                metadata.set(property, decodePDFEncoding(v));
            }
        }
    }

    /**
     * Add metadata property unless value is null.
     * If property does not allow multiple values, this will set=overwrite any
     * existing value with the new value
     *
     * @param metadata
     * @param property
     * @param value
     */
    private void addMetadata(Metadata metadata, Property property, String value) {
        if (value != null) {
            if (property.isMultiValuePermitted()) {
                metadata.add(property, decodePDFEncoding(value));
            } else {
                metadata.set(property, decodePDFEncoding(value));
            }
        }
    }

    private void addMetadata(Metadata metadata, String name, String value) {
        if (value != null) {
            metadata.add(name, decodePDFEncoding(value));
        }
    }

    private void addMetadata(Metadata metadata, String name, Calendar value) {
        if (value != null) {
            metadata.set(name, value.getTime().toString());
        }
    }

    private void addMetadata(Metadata metadata, Property property, Calendar value) {
        if (value != null) {
            metadata.set(property, value.getTime());
        }
    }

    /**
     * Used when processing custom metadata entries, as PDFBox won't do
     * the conversion for us in the way it does for the standard ones
     */
    private void addMetadata(Metadata metadata, String name, COSBase value) {
        if (value instanceof COSArray) {
            for (Object v : ((COSArray) value).toList()) {
                addMetadata(metadata, name, ((COSBase) v));
            }
        } else if (value instanceof COSString) {
            addMetadata(metadata, name, ((COSString) value).getString());
        }
        // Avoid calling COSDictionary#toString, since it can lead to infinite
        // recursion. See TIKA-1038 and PDFBOX-1835.
        else if (value != null && !(value instanceof COSDictionary)) {
            addMetadata(metadata, name, value.toString());
        }
    }


    private String decodePDFEncoding(String value) {
        if (value == null) {
            return "";
        }
        System.out.println("DECODE: "+value);
        System.out.println("SHOULD: "+PDFOctalUnicodeDecoder.shouldDecode(value));
        if (PDFOctalUnicodeDecoder.shouldDecode(value)) {
            PDFOctalUnicodeDecoder d = new PDFOctalUnicodeDecoder();
            return d.decode(value);
        }
        return value;
    }

    public PDFParserConfig getPDFParserConfig() {
        return defaultConfig;
    }

    public void setPDFParserConfig(PDFParserConfig config) {
        this.defaultConfig = config;
    }

    /**
     * @see #setEnableAutoSpace(boolean)
     * @deprecated use {@link #getPDFParserConfig()}
     */
    public boolean getEnableAutoSpace() {
        return defaultConfig.getEnableAutoSpace();
    }

    /**
     * If true (the default), the parser should estimate
     * where spaces should be inserted between words.  For
     * many PDFs this is necessary as they do not include
     * explicit whitespace characters.
     *
     * @deprecated use {@link #setPDFParserConfig(PDFParserConfig)}
     */
    public void setEnableAutoSpace(boolean v) {
        defaultConfig.setEnableAutoSpace(v);
    }

    /**
     * If true, text in annotations will be extracted.
     *
     * @deprecated use {@link #getPDFParserConfig()}
     */
    public boolean getExtractAnnotationText() {
        return defaultConfig.getExtractAnnotationText();
    }

    /**
     * If true (the default), text in annotations will be
     * extracted.
     *
     * @deprecated use {@link #setPDFParserConfig(PDFParserConfig)}
     */
    public void setExtractAnnotationText(boolean v) {
        defaultConfig.setExtractAnnotationText(v);
    }

    /**
     * @see #setSuppressDuplicateOverlappingText(boolean)
     * @deprecated use {@link #getPDFParserConfig()}
     */
    public boolean getSuppressDuplicateOverlappingText() {
        return defaultConfig.getSuppressDuplicateOverlappingText();
    }

    /**
     * If true, the parser should try to remove duplicated
     * text over the same region.  This is needed for some
     * PDFs that achieve bolding by re-writing the same
     * text in the same area.  Note that this can
     * slow down extraction substantially (PDFBOX-956) and
     * sometimes remove characters that were not in fact
     * duplicated (PDFBOX-1155).  By default this is disabled.
     *
     * @deprecated use {@link #setPDFParserConfig(PDFParserConfig)}
     */
    public void setSuppressDuplicateOverlappingText(boolean v) {
        defaultConfig.setSuppressDuplicateOverlappingText(v);
    }

    /**
     * @see #setSortByPosition(boolean)
     * @deprecated use {@link #getPDFParserConfig()}
     */
    public boolean getSortByPosition() {
        return defaultConfig.getSortByPosition();
    }

    /**
     * If true, sort text tokens by their x/y position
     * before extracting text.  This may be necessary for
     * some PDFs (if the text tokens are not rendered "in
     * order"), while for other PDFs it can produce the
     * wrong result (for example if there are 2 columns,
     * the text will be interleaved).  Default is false.
     *
     * @deprecated use {@link #setPDFParserConfig(PDFParserConfig)}
     */
    public void setSortByPosition(boolean v) {
        defaultConfig.setSortByPosition(v);
    }

}
