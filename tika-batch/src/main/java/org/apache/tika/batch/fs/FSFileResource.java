package org.apache.tika.batch.fs;

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

import org.apache.tika.batch.FileResource;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.util.BatchLocalization;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * FileSystem(FS)Resource wraps a file name.
 * <p/>
 * This class automatically sets the following keys in Metadata:
 * <ul>
 *     <li>Metadata.RESOURCE_NAME_KEY (file name)</li>
 *     <li>Metadata.CONTENT_LENGTH</li>
 *     <li>FSProperties.FS_ABSOLUTE_PATH</li>
 *     <li>FileResource.FILE_EXTENSION</li>
 * </ul>,
 */
public class FSFileResource implements FileResource {

    private final File fullPath;
    private final Metadata metadata;

    public FSFileResource(File srcRoot, File fullPath) {
        this.fullPath = fullPath;
        this.metadata = new Metadata();
        //child path must actually be a child
        assert(FSUtil.checkThisIsAncestorOfThat(srcRoot, fullPath));
        String relativePath = fullPath.getAbsolutePath().substring(srcRoot.getAbsolutePath().length()+1);

        //need to set these now so that the filter can determine
        //whether or not to crawl this file
        metadata.set(Metadata.RESOURCE_NAME_KEY, fullPath.getName());
        metadata.set(Metadata.CONTENT_LENGTH, Long.toString(fullPath.length()));
        metadata.set(FSProperties.FS_ABSOLUTE_PATH, fullPath.getAbsolutePath());
        metadata.set(FSProperties.FS_REL_PATH, relativePath);
        metadata.set(FileResource.FILE_EXTENSION, getExtension(fullPath));
    }

    /**
     * Simple extension extractor that takes whatever comes after the
     * last period in the path.  It returns a lowercased version of that.
     * <p/>
     * If there is no period, it returns an empty string.
     *
     * @param fullPath
     * @return
     */
    private String getExtension(File fullPath) {
        String p = fullPath.getName();
        int i = p.lastIndexOf(".");
        if (i > -1) {
            return p.substring(i + 1).toLowerCase(BatchLocalization.getLocale());
        }
        return "";
    }

    /**
     *
     * @return file's absolutePath
     */
    @Override
    public String getResourceId() {
        return fullPath.getAbsolutePath();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public InputStream openInputStream() throws IOException {
        //no need to include Metadata because we already set the
        //same information in the initializer
        return TikaInputStream.get(fullPath);
    }
}
