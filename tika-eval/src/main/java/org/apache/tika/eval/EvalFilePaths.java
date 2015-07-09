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

package org.apache.tika.eval;

import java.io.File;

/**
 * Simple struct to keep track of source file (original binary file, e.g. document1.doc)
 * and the extract file (e.g. document1.doc.json).
 */
class EvalFilePaths {

    String sourceFileName;
    String relativeSourceFilePath;
    File extractFile;
    long sourceFileLength = -1l;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EvalFilePaths that = (EvalFilePaths) o;

        if (sourceFileLength != that.sourceFileLength) return false;
        if (sourceFileName != null ? !sourceFileName.equals(that.sourceFileName) : that.sourceFileName != null)
            return false;
        if (relativeSourceFilePath != null ? !relativeSourceFilePath.equals(that.relativeSourceFilePath) : that.relativeSourceFilePath != null)
            return false;
        return !(extractFile != null ? !extractFile.equals(that.extractFile) : that.extractFile != null);

    }

    @Override
    public int hashCode() {
        int result = sourceFileName != null ? sourceFileName.hashCode() : 0;
        result = 31 * result + (relativeSourceFilePath != null ? relativeSourceFilePath.hashCode() : 0);
        result = 31 * result + (extractFile != null ? extractFile.hashCode() : 0);
        result = 31 * result + (int) (sourceFileLength ^ (sourceFileLength >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "EvalFilePaths{" +
                "sourceFileName='" + sourceFileName + '\'' +
                ", relativeSourceFilePath='" + relativeSourceFilePath + '\'' +
                ", extractFile=" + extractFile +
                ", sourceFileLength=" + sourceFileLength +
                '}';
    }
}
