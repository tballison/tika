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

package org.apache.tika.batch.fs;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to handle some common issues when
 * reading from and writing to a file system (FS).
 */
public class FSUtil {

    public static boolean checkThisIsAncestorOfThat(File ancestor, File child) {
        int ancLen = ancestor.getAbsolutePath().length();
        int childLen = child.getAbsolutePath().length();
        if (childLen <= ancLen) {
            return false;
        }

        String childBase = child.getAbsolutePath().substring(0, ancLen);
        return childBase.equals(ancestor.getAbsolutePath());

    }

    public static boolean checkThisIsAncestorOfOrSameAsThat(File ancestor, File child) {
        if (ancestor.equals(child)) {
            return true;
        }
        return checkThisIsAncestorOfThat(ancestor, child);
    }

    public enum HANDLE_EXISTING {
        OVERWRITE,
        RENAME,
        SKIP
    }

    private final static Pattern FILE_NAME_PATTERN =
            Pattern.compile("\\A(.*?)(?:\\((\\d+)\\))?\\.([^\\.]+)\\Z");

    /**
     * Given a target root and an initial relative path,
     * return the target file according to the HANDLE_EXISTING strategy
     * <p/>
     * In the most basic use case, given a root directory "input",
     * a file's relative path "dir1/dir2/fileA.docx", and a target directory
     * "output", the target file would be "output/dir1/dir2/fileA.docx."
     * <p/>
     * If HANDLE_EXISTING is set to OVERWRITE, this will not check to see if the target already exists,
     * and the returned file could overwrite an existing file!!!
     * <p/>
     * If HANDLE_EXISTING is set to RENAME, this will try to increment a counter at the end of
     * the file name (fileA(2).docx) until there is a file name that doesn't exist.
     * <p/>
     * This will return null if handleExisting == HANDLE_EXISTING.SKIP and
     * the candidate file already exists.
     * <p/>
     * This will throw an IOException if HANDLE_EXISTING is set to
     * RENAME, and a candidate cannot target file cannot be found
     * after trying to increment the file count (e.g. fileA(2).docx) 10000 times
     * and then after trying 20,000 UUIDs.
     *
     * @param targetRoot directory root for target
     * @param initialRelativePath initial relative path (including file name, which may be renamed)
     * @param handleExisting what to do if the target file exists
     * @param suffix suffix to add to files, can be null
     * @return target file or null if no target file should be created
     * @throws java.io.IOException
     */
    public static File getTargetFile(File targetRoot, String initialRelativePath,
                                     HANDLE_EXISTING handleExisting, String suffix) throws IOException {
        String localSuffix = (suffix == null) ? "" : suffix;
        File cand = new File(targetRoot, initialRelativePath+"."+localSuffix);
        if (cand.isFile()) {
            if (handleExisting.equals(HANDLE_EXISTING.OVERWRITE)) {
                return cand;
            } else if (handleExisting.equals(HANDLE_EXISTING.SKIP)) {
                return null;
            }
        }

        //if we're here, the target file exists, and
        //we must find a new name for it.

        //groups for "testfile(1).txt":
        //group(1) is "testfile"
        //group(2) is 1
        //group(3) is "txt"
        //Note: group(2) can be null
        int cnt = 0;
        String fNameBase = null;
        String fNameExt = "";
        //this doesn't include the addition of the localSuffix
        File candOnly = new File(targetRoot, initialRelativePath);
        Matcher m = FILE_NAME_PATTERN.matcher(candOnly.getName());
        if (m.find()) {
            fNameBase = m.group(1);

            if (m.group(2) != null) {
                try {
                    cnt = Integer.parseInt(m.group(2));
                } catch (NumberFormatException e) {
                    //swallow
                }
            }
            if (m.group(3) != null) {
                fNameExt = m.group(3);
            }
        }

        File targetParent = cand.getParentFile();
        while (fNameBase != null && cand.isFile() && ++cnt < 10000) {
            String candFileName = fNameBase + "(" + cnt + ")." + fNameExt+"."+localSuffix;
            cand = new File(targetParent, candFileName);
        }
        //reset count to 0 and try 20000 times
        cnt = 0;
        while (cand.isFile() && cnt++ < 20000) {
            UUID uid = UUID.randomUUID();
            cand = new File(targetParent, uid.toString() + fNameExt+"."+localSuffix);
        }

        if (cand.isFile()) {
            throw new IOException("Couldn't find candidate target file after trying " +
                    "very, very hard");
        }
        return cand;
    }

}