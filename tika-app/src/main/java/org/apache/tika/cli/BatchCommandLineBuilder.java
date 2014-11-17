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

package org.apache.tika.cli;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This takes a TikaCLI commandline and builds the full commandline for
 * org.apache.tika.batch.fs.FSBatchProcessCLI.
 */
class BatchCommandLineBuilder {

    static Pattern JVM_OPTS_PATTERN = Pattern.compile("^(--?)J(.+)");

    protected static String[] build(String[] args) throws IOException {
        Map<String, String> processArgs = new LinkedHashMap<String, String>();
        Map<String, String> jvmOpts = new LinkedHashMap<String,String>();
        mapifyArgs(args, processArgs, jvmOpts);

        List<String> translatedProcessArgs = translateCommandLine(args, processArgs);

        //maybe the user specified a different classpath?!
        if (! jvmOpts.containsKey("-cp") && ! jvmOpts.containsKey("--classpath")) {
            String cp = System.getProperty("java.class.path");
            //need to test for " " on *nix, can't just add double quotes
            //across platforms.
            if (cp.contains(" ")){
                cp = "\""+cp+"\"";
            }
            jvmOpts.put("-cp", cp);
        }

        //now build the full command line
        List<String> fullCommand = new ArrayList<String>();
        fullCommand.add("java");
        for (Map.Entry<String, String> e : jvmOpts.entrySet()) {
            fullCommand.add(e.getKey());
            if (e.getValue().length() > 0) {
                fullCommand.add(e.getValue());
            }
        }
        fullCommand.add("org.apache.tika.batch.fs.FSBatchProcessCLI");

        for (Map.Entry<String, String> e : processArgs.entrySet()) {
            fullCommand.add(e.getKey());
            if (e.getValue().length() > 0) {
                fullCommand.add(e.getValue());
            }
        }
        return fullCommand.toArray(new String[fullCommand.size()]);
    }


    private static void mapifyArgs(final String[] args,
                                   final Map<String, String> commandLine,
                                   final Map<String, String> jvmArgs) {

        if (args.length == 0) {
            return;
        }
        //need special handling in case last element in args is the src directory
        //if it is, then don't look for args in the last place of the args[].

        int end = args.length;
        String srcDirString = "";
        File srcDir = new File(args[args.length-1]);
        if (srcDir.isDirectory()) {
            end = args.length-1;
            srcDirString = args[args.length-1];
        }
        Matcher matcher = JVM_OPTS_PATTERN.matcher("");
        for (int i = 0; i < end; i++) {
            if (matcher.reset(args[i]).find()) {
                String jvmArg = matcher.group(1)+matcher.group(2);
                String v = "";
                if (i < end-1 && ! args[i+1].startsWith("-")){
                    v = args[i+1];
                    i++;
                }
                jvmArgs.put(jvmArg, v);
            } else if (args[i].startsWith("-")) {
                String k = args[i];
                String v = "";
                if (i < end-1 && ! args[i+1].startsWith("-")){
                    v = args[i+1];
                    i++;
                }
                commandLine.put(k, v);
            }
        }
        //if we defined this above, now overwrite whatever may have been
        //parsed as -srcDir earlier
        if (srcDirString.length() > 0) {
            commandLine.put("-srcDir", srcDirString);
        }
    }


    private static List<String> translateCommandLine(String[] args, Map<String, String> map) throws IOException {
        //if no -srcDir is specified, but the last
        //item in the list is a directory, treat that as srcDir
        if (! map.containsKey("-srcDir")) {
            File tmpFile = new File(args[args.length-1]);
            if (tmpFile.isDirectory()) {
                map.put("-srcDir", args[args.length-1]);
            }
        }

        //now translate output types
        if (map.containsKey("-h") || map.containsKey("--html")) {
            map.remove("-h");
            map.remove("--html");
            map.put("-basicHandlerType", "html");
            map.put("-targetSuffix", "html");
        } else if (map.containsKey("-x") || map.containsKey("--xml")) {
            map.remove("-x");
            map.remove("--xml");
            map.put("-basicHandlerType", "xml");
            map.put("-targetSuffix", "xml");
        } else if (map.containsKey("-t") || map.containsKey("--text")) {
            map.remove("-t");
            map.remove("--text");
            map.put("-basicHandlerType", "text");
            map.put("-targetSuffix", "txt");
        } else if (map.containsKey("-m") || map.containsKey("--metadata")) {
            map.remove("-m");
            map.remove("--metadata");
            map.put("-basicHandlerType", "ignore");
            map.put("-targetSuffix", "json");
        } else if (map.containsKey("-T") || map.containsKey("--text-main")) {
            map.remove("-T");
            map.remove("--text-main");
            map.put("-basicHandlerType", "body");
            map.put("-targetSuffix", "txt");
        }

        if (map.containsKey("-J") || map.containsKey("--jsonRecursive")) {
            map.remove("-J");
            map.remove("--jsonRecursive");
            map.put("-recursiveParserWrapper", "true");
            //overwrite targetSuffix
            map.put("-targetSuffix", "json");
        }
        //package
        List<String> translated = new ArrayList<String>();
        for (Map.Entry<String, String> e : map.entrySet()) {
            translated.add(e.getKey());
            if (e.getValue() != null && e.getValue().trim().length() > 0) {
                translated.add(e.getValue());
            }
        }
        return translated;
    }
}
