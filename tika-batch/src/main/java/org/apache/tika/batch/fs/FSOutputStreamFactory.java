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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.tika.batch.OutputStreamFactory;
import org.apache.tika.metadata.Metadata;

public class FSOutputStreamFactory implements OutputStreamFactory {

    public enum COMPRESSION {
        NONE,
        BZIP2,
        GZIP,
        ZIP
    }
    private final FSUtil.HANDLE_EXISTING handleExisting;
    private final File targetRoot;
    private final String suffix;
    private final COMPRESSION compression;

    public FSOutputStreamFactory(File targetRoot, FSUtil.HANDLE_EXISTING handleExisting,
                                 COMPRESSION compression, String suffix) {
        this.handleExisting = handleExisting;
        this.targetRoot = targetRoot.getAbsoluteFile();
        this.suffix = suffix;
        this.compression = compression;
    }

    /**
     * This tries to create a file based on the {@link org.apache.tika.batch.fs.FSUtil.HANDLE_EXISTING}
     * value that was passed in during initialization.
     * <p>
     * If {@link #handleExisting} is set to "SKIP" and the target file already exists,
     * this will return null.
     * <p>
     * If a target file can be found, this will try to mkdirs for that target file.
     * If mkdirs() fails, this will throw an IOException.
     * <p>
     * Finally, this will open an output stream for the appropriate target file.
     * @param metadata must have a value set for FSMetadataProperties.FS_ABSOLUTE_PATH or
     *                 else NullPointerException will be thrown!
     * @return
     * @throws java.io.IOException, NullPointerException
     */
    @Override
    public OutputStream getOutputStream(Metadata metadata) throws IOException {
        String initialRelativePath = metadata.get(FSProperties.FS_REL_PATH);
        File targetFile = FSUtil.getTargetFile(targetRoot, initialRelativePath, handleExisting, suffix);
        if (targetFile == null) {
            return null;
        }
        if (! targetFile.getParentFile().isDirectory()) {
            boolean success = targetFile.getParentFile().mkdirs();
            //with multithreading, it is possible that the parent file was created between
            //the test and the attempt to .mkdirs(); mkdirs() returns false if the dirs already exist
            if (! success && ! targetFile.getParentFile().isDirectory()) {
                throw new IOException("Couldn't create parent directory for:"+targetFile.getAbsolutePath());
            }
        }

        OutputStream os = new FileOutputStream(targetFile);
        if (compression == COMPRESSION.BZIP2){
            os = new BZip2CompressorOutputStream(os);
        } else if (compression == COMPRESSION.GZIP) {
            os = new GZIPOutputStream(os);
        } else if (compression == COMPRESSION.ZIP) {
            os = new ZipArchiveOutputStream(os);
        }
        return os;
    }
}
