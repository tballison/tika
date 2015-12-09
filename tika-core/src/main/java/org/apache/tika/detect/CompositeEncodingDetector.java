package org.apache.tika.detect;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.tika.config.LoadErrorHandler;
import org.apache.tika.config.ServiceLoader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.utils.CharsetUtils;

public class CompositeEncodingDetector {

    /**
     *
     * @param input input stream
     * @param metadata metadata that may contain info about the encoding
     * @param detectors list of detectors to use; first non-null value is returned
     * @param handler what to do if there's a NoClassDefFoundError
     * @return
     * @throws IOException
     * @throws TikaException
     */
    public static Charset detectCharset(
            InputStream input, Metadata metadata,
            List<EncodingDetector> detectors, LoadErrorHandler handler)
            throws IOException, TikaException {
        // Ask all given detectors for the character encoding
        for (EncodingDetector detector : detectors) {
            try {
                Charset charset = detector.detect(input, metadata);
                if (charset != null) {
                    return charset;
                }
            } catch (NoClassDefFoundError e) {
                // TIKA-1041: Detector dependencies not present.
                handler.handleLoadError(detector.getClass().getName(), e);
            }
        }

        // Try determining the encoding based on hints in document metadata
        MediaType type = MediaType.parse(metadata.get(Metadata.CONTENT_TYPE));
        if (type != null) {
            String charset = type.getParameters().get("charset");
            if (charset != null) {
                try {
                    return CharsetUtils.forName(charset);
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        throw new TikaException(
                "Failed to detect the character encoding of a document");
    }

    /**
     * Convenience method that loads detectors from EncodingDetector.
     *
     * @param stream
     * @param metadata
     * @param loader
     * @return
     * @throws IOException
     * @throws TikaException
     */
    public static Charset detectCharset(
            InputStream stream, Metadata metadata,
            ServiceLoader loader) throws IOException, TikaException {

        return detectCharset(stream, metadata,
                loader.loadServiceProviders(EncodingDetector.class),
                loader.getLoadErrorHandler());
    }
}
