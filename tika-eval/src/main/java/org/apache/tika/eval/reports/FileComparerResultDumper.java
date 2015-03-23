package org.apache.tika.eval.reports;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.IOUtils;
import org.apache.tika.eval.db.H2Util;
import org.apache.tika.eval.io.JDBCTableWriter;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.ToHTMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * This is currently an abomination unto sql bloat.
 * This is only being used for development.  The next step is to
 * parameterize the reports that are written.
 */
public class FileComparerResultDumper {

    /**
     * This relies on java aliases that are h2 specific:
     * DEC_PATTERN converts decimal to string with 2 decimal places
     */

    //temp table names
    String extensions_total_table = "extensions_total";
    String detected_types_A_table = "detected_types_A";
    String detected_types_B_table = "detected_types_B";
    String exceptions_A_table = "exceptions_A";
    String exceptions_B_table = "exceptions_B";
    String millis_A_table = "millis_A";
    String millis_B_table = "millis_B";


    private void execute(File dbFile, File outputDir) throws IOException, SQLException, SAXException {
        H2Util util = new H2Util();

        System.out.println(util.getJDBCDriverClass());
        Connection connection = util.getConnection(dbFile);
        Statement st = connection.createStatement();
        setAliases(st);
        String[] dirNames = getDirNames(connection);
        String dirNameA = dirNames[0];
        String dirNameB = dirNames[1];
        createTempTables(st);
        try {

            System.out.println("Metadata values");
            dumpMetadata(dirNameA, dirNameB, outputDir, st);
            System.out.println("Detected langs");
            dumpLangs(dirNameA, dirNameB, outputDir, st);
            System.out.println("Stack traces");
            dumpStackTraces(dirNameA, dirNameB, outputDir, st);
            System.out.println("Exceptions");
            dumpExceptions(dirNameA, dirNameB, outputDir, st);
            System.out.println("Attachments");
            dumpAttachments(dirNameA, dirNameB, outputDir, st);
            System.out.println("Mime types");
            dumpMimes(dirNameA, dirNameB, outputDir, st);
            System.out.println("File contents");
            dumpDiffContents(dirNameA, dirNameB, outputDir, st);
            System.out.println("Processing times");
            dumpTimes(dirNameA, dirNameB, outputDir, st);

        } finally {
            st.close();
            connection.close();
        }
    }

    private void setAliases(Statement st) throws SQLException {
        String sql = "drop alias if exists DEC_PATTERN;\n" +
                "create alias DEC_PATTERN as $$\n" +
                "String toChar(BigDecimal x, String pattern) throws Exception {\n" +
                "     return new java.text.DecimalFormat(pattern).format(x);}\n" +
                " $$;";
        st.execute(sql);
    }

    private void dumpMetadata(String dirNameA, String dirNameB, File outputDir, Statement st)
            throws SQLException, IOException, SAXException {
        File metadataDir = new File(outputDir, "metadata");
        metadataDir.mkdirs();
        String sql = "SELECT DETECTED_FILE_EXTENSION_B, count(1) as COUNT "+
                " from comparisons "+
                "where JSON_EX_A is null and JSON_EX_B is null and "+
                "SORT_STACK_TRACE_A is null and SORT_STACK_TRACE_B is null and "+
                "DIFF_NUM_ATTACHMENTS = 0 and "+
                "DIFF_NUM_METADATA_VALUES < 0 "+
                "group by DETECTED_FILE_EXTENSION_B";

        dumpTable(metadataDir, "fewer_metadata_values_in_B_by_detected_extension.html",
                "Files with fewer metadata values in \"" + dirNameB + "\" than in \""+dirNameA+
                        "\" by detected file extension",
                sql, st);

        sql = "SELECT DETECTED_FILE_EXTENSION_A, count(1) as COUNT "+
                " from comparisons "+
                "where JSON_EX_A is null and JSON_EX_B is null and "+
                "SORT_STACK_TRACE_A is null and SORT_STACK_TRACE_B is null and "+
                "DIFF_NUM_ATTACHMENTS = 0 and "+
                "DIFF_NUM_METADATA_VALUES > 0 "+
                "group by DETECTED_FILE_EXTENSION_A";

        dumpTable(metadataDir, "fewer_metadata_values_in_A_by_detected_extension.html",
                "Files with fewer metadata values in \"" + dirNameA + "\" than in \""+dirNameB+
                        "\" by detected file extension",
                sql, st);

        sql = "SELECT comparisons.detected_content_type_A, " +
                "sum(ifnull(NUM_METADATA_VALUES_A, 0)) as NUM_METADATA_VALUES_TOTAL, " +
                detected_types_A_table +".NUM_FILES as TOTAL_FILES, "+
                "DEC_PATTERN(" +
                "(1.0*sum(ifnull(NUM_METADATA_VALUES_A, 0))/"+detected_types_A_table+".NUM_FILES)" +
                ", '#.##' ) as "+
                "\"Average number of metadata values per file\" "+
                "from comparisons " +
                "left outer join "+detected_types_A_table+" on comparisons.detected_content_type_A="
                +detected_types_A_table+".DETECTED_CONTENT_TYPE_A "+
                "group by comparisons.DETECTED_CONTENT_TYPE_A " +
                "order by NUM_METADATA_VALUES_TOTAL desc;";


        dumpTable(metadataDir, "metadata_values_in_A_by_content_type.html",
                "Number of metadata values per content type in \"" + dirNameA+"\"",
                sql, st);

        sql = "SELECT comparisons.detected_content_type_B as \"Detected Content Type B\", " +
                "DEC_PATTERN(sum(ifnull(NUM_METADATA_VALUES_B, 0)), '###,###') as \"Total Number of Metadata Values\", " +
                "DEC_PATTERN("+detected_types_B_table +".NUM_FILES, '###,###') as \"Total Number of Files\", "+
                "DEC_PATTERN(" +
                "(1.0*sum(ifnull(NUM_METADATA_VALUES_B, 0))/"+detected_types_B_table+".NUM_FILES)"+
                ", '###,###') as "+
                "\"Average Number of Metadata Values per File\" "+
                "from comparisons " +
                "left outer join "+detected_types_B_table+" on comparisons.detected_content_type_B="
                +detected_types_B_table+".DETECTED_CONTENT_TYPE_B "+
                "group by comparisons.DETECTED_CONTENT_TYPE_B " +
                "order by sum(ifnull(NUM_METADATA_VALUES_B, 0)) desc;";


        dumpTable(metadataDir, "metadata_values_in_B_by_content_type.html",
                "Number of Metadata Values per Content Type in \"" + dirNameA+"\"",
                sql, st);

    }


    private void createTempTables(Statement st) throws SQLException {
        //create seven tmp tables, one for the total number of files by extension
        //one each for exceptions in a and exceptions in b
        //one each for detected types in a and b
        //one each for times per detected type in a and b

        String sql = "";
        String[] tmpTables = new String[]{
                extensions_total_table,
                exceptions_A_table,
                exceptions_B_table,
                detected_types_A_table,
                detected_types_B_table,
                millis_A_table,
                millis_B_table
        };

        for (String table : tmpTables) {
            sql = "drop table if exists "+table;
            st.execute(sql);
        }

        sql = "CREATE TEMP table "+extensions_total_table+" ("+
                "FILE_EXTENSION VARCHAR(64), "+
                "NUM_FILES INTEGER)";
        st.execute(sql);

        sql = "create temp table "+exceptions_A_table+" ("+
                "FILE_EXTENSION VARCHAR(64),"+
                "NUM_CAUGHT_EXCEPTIONS_A INTEGER);";
        st.execute(sql);

        sql = "create temp table "+exceptions_B_table+" ("+
                "FILE_EXTENSION VARCHAR(64),"+
                "NUM_CAUGHT_EXCEPTIONS_B INTEGER);";
        st.execute(sql);

        sql = "CREATE TEMP table "+detected_types_A_table+" ("+
                "DETECTED_CONTENT_TYPE_A VARCHAR(128), "+
                "NUM_FILES INTEGER)";
        st.execute(sql);

        sql = "CREATE TEMP table "+detected_types_B_table+" ("+
                "DETECTED_CONTENT_TYPE_B VARCHAR(128), "+
                "NUM_FILES INTEGER)";
        st.execute(sql);

        sql = "CREATE TEMP table "+millis_A_table+" ("+
                "DETECTED_CONTENT_TYPE_A VARCHAR(128), "+
                "ELAPSED_TIME_MILLIS_A LONG)";
        st.execute(sql);

        sql = "CREATE TEMP table "+millis_B_table+" ("+
                "DETECTED_CONTENT_TYPE_B VARCHAR(128), "+
                "ELAPSED_TIME_MILLIS_B LONG)";
        st.execute(sql);

        //now insert the data
        sql = "INSERT into "+extensions_total_table+" (FILE_EXTENSION, NUM_FILES)" + "" +
                "select FILE_EXTENSION, count(1) from comparisons " +
                "group by file_extension;";
        st.execute(sql);

        sql = "INSERT into "+exceptions_A_table+" (FILE_EXTENSION, NUM_CAUGHT_EXCEPTIONS_A) " +
                "select FILE_EXTENSION, count(1) from comparisons where " +
                "sort_stack_trace_a is not null "+
                "group by file_extension;";
        st.execute(sql);

        sql = "INSERT into "+exceptions_B_table+" (FILE_EXTENSION, NUM_CAUGHT_EXCEPTIONS_B) " +
                "select FILE_EXTENSION, count(1) from comparisons where " +
                "sort_stack_trace_b is not null " +
                "group by file_extension;";
        st.execute(sql);

        sql = "INSERT into "+detected_types_A_table+" (DETECTED_CONTENT_TYPE_A, NUM_FILES) " +
                "select DETECTED_CONTENT_TYPE_A, count(1) from comparisons "+
                "group by DETECTED_CONTENT_TYPE_A;";
        st.execute(sql);

        sql = "INSERT into "+detected_types_B_table+" (DETECTED_CONTENT_TYPE_B, NUM_FILES) " +
                "select DETECTED_CONTENT_TYPE_B, count(1) from comparisons "+
                "group by DETECTED_CONTENT_TYPE_B;";
        st.execute(sql);

        sql = "INSERT into "+millis_A_table +"(DETECTED_CONTENT_TYPE_A, ELAPSED_TIME_MILLIS_A) "+
                "select DETECTED_CONTENT_TYPE_A, sum(ELAPSED_TIME_MILLIS_A) "+
                "from comparisons "+
                "where JSON_EX_A is null and JSON_EX_B is null and SORT_STACK_TRACE_A is null and "+
                "SORT_STACK_TRACE_B is null "+
                "group by DETECTED_CONTENT_TYPE_A";
        st.execute(sql);

        sql = "INSERT into "+millis_B_table +"(DETECTED_CONTENT_TYPE_B, ELAPSED_TIME_MILLIS_B) "+
                "select DETECTED_CONTENT_TYPE_B, sum(ELAPSED_TIME_MILLIS_B) "+
                "from comparisons "+
                "where JSON_EX_A is null and JSON_EX_B is null and SORT_STACK_TRACE_A is null and "+
                "SORT_STACK_TRACE_B is null "+
                "group by DETECTED_CONTENT_TYPE_B";
        st.execute(sql);

    }

    private void dumpTimes(String dirNameA, String dirNameB, File outputDir, Statement st) throws SQLException,
            IOException, SAXException {
        File timesDir = new File(outputDir, "time");
        timesDir.mkdirs();

        String sql = "SELECT comparisons.FILE_EXTENSION, "+
                "sum(ifnull(ELAPSED_TIME_MILLIS_A, 0)) as \"Elapsed Milliseconds A\", " +
                extensions_total_table +".NUM_FILES as \"Total Number of Files\", "+
                "DEC_PATTERN(" +
                "(1.0*sum(ifnull(ELAPSED_TIME_MILLIS_A, 0))/"+extensions_total_table+".NUM_FILES)"+
                ", '###,###.#')"+
                " as \"Average Number of Milliseconds per File\" "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "where JSON_EX_A is null and SORT_STACK_TRACE_A is null "+
                "group by comparisons.FILE_EXTENSION " +
                "order by \"Average Number of Milliseconds per File\" desc;";
        dumpTable(timesDir, "time_millis_in_A_by_extension.html",
                "Total number of milliseconds for \"" + dirNameA + "\" by file extension",
                sql, st);

        sql = "SELECT comparisons.FILE_EXTENSION, sum(ifnull(ELAPSED_TIME_MILLIS_B, 0)) as \"Elapsed Time(Millis) B\", " +
                extensions_total_table +".NUM_FILES as TOTAL_FILES, "+
                "DEC_PATTERN(" +
                "(1.0*sum(ifnull(ELAPSED_TIME_MILLIS_B, 0))/"+extensions_total_table+".NUM_FILES)"+
                ", '###,###.#')"+
                " as \"Average Number of Milliseconds per File\" "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "where JSON_EX_B is null and SORT_STACK_TRACE_B is null "+
                "group by comparisons.FILE_EXTENSION " +
                "order by \"Average Number of Milliseconds per File\" desc;";
        dumpTable(timesDir, "time_millis_in_B_by_extension.html",
                "Total number of milliseconds for \"" + dirNameB + "\" by file extension",
                sql, st);

        sql = "select "+millis_A_table+".DETECTED_CONTENT_TYPE_A, "+millis_A_table+".ELAPSED_TIME_MILLIS_A, "+
                detected_types_A_table+".NUM_FILES, "+
                 millis_B_table+".ELAPSED_TIME_MILLIS_B, " +
                detected_types_B_table+".NUM_FILES"+
                " from " + millis_A_table +
                " join " + millis_B_table + " on " + millis_A_table+".DETECTED_CONTENT_TYPE_A=" +
                    millis_B_table+".DETECTED_CONTENT_TYPE_B " +
                " join " + detected_types_A_table + " on "+millis_B_table+".DETECTED_CONTENT_TYPE_B="+
                    detected_types_A_table+".DETECTED_CONTENT_TYPE_A" +
                " join " + detected_types_B_table + " on "+millis_B_table+".DETECTED_CONTENT_TYPE_B="+
                detected_types_B_table+".DETECTED_CONTENT_TYPE_B";
        dumpTable(timesDir, "time_millis_in_A_vs_B_detected_type.html",
                "Comparison of total time to process each file type in \"" + dirNameA + "\" vs. \""+dirNameB+"\"",
                sql, st);


    }

    private void dumpLangs(String dirNameA, String dirNameB, File outputDir, Statement st) throws SQLException,
            IOException, SAXException {
        File langsDir = new File(outputDir, "detected_langs");
        langsDir.mkdirs();

        String sql = "select LANG_ID1_A, count(1) as COUNT "+
                "from comparisons "+
                "where LANG_ID1_A is not null "+
                "group by LANG_ID1_A "+
                "order by COUNT desc ";
        dumpTable(langsDir, "languages_in_A.html",
                "Languages detected in " + dirNameA,
                sql, st);

        sql = "select LANG_ID1_B, count(1) as COUNT "+
                "from comparisons "+
                "where LANG_ID1_B is not null "+
                "group by LANG_ID1_B "+
                "order by COUNT desc ";
        dumpTable(langsDir, "languages_in_B.html",
                "Languages detected in " + dirNameA,
                sql, st);

        sql = "select DETECTED_CONTENT_TYPE_A, LANG_ID1_A, count(1) as COUNT "+
                "from comparisons "+
                "where LANG_ID1_A is not null "+
                "group by DETECTED_CONTENT_TYPE_A, LANG_ID1_A "+
                "order by DETECTED_CONTENT_TYPE_A, COUNT desc ";
        dumpTable(langsDir, "languages_in_A_by_content_type_lang.html",
                "Languages detected in " + dirNameA + " by content type then language",
                sql, st);

        sql = "select LANG_ID1_A, DETECTED_CONTENT_TYPE_A, count(1) as COUNT "+
                "from comparisons "+
                "where LANG_ID1_A is not null "+
                "group by DETECTED_CONTENT_TYPE_A, LANG_ID1_A "+
                "order by lang_ID1_A, COUNT desc, DETECTED_CONTENT_TYPE_A;";

        dumpTable(langsDir, "languages_in_A_by_lang_content_type.html",
                "Languages detected in " + dirNameA + " by language then content type",
                sql, st);

        sql = "select DETECTED_CONTENT_TYPE_B, LANG_ID1_B, count(1) as COUNT "+
                "from comparisons "+
                "where LANG_ID1_B is not null "+
                "group by DETECTED_CONTENT_TYPE_B, LANG_ID1_B "+
                "order by DETECTED_CONTENT_TYPE_B, COUNT desc ";
        dumpTable(langsDir, "languages_in_B_by_content_type_lang.html",
                "Languages detected in " + dirNameB + " by content type then language",
                sql, st);

        sql = "select LANG_ID1_B, DETECTED_CONTENT_TYPE_B, count(1) as COUNT "+
                "from comparisons "+
                "where LANG_ID1_B is not null "+
                "group by DETECTED_CONTENT_TYPE_B, LANG_ID1_B "+
                "order by lang_ID1_B, COUNT desc, DETECTED_CONTENT_TYPE_B;";

        dumpTable(langsDir, "languages_in_B_by_lang_content_type.html",
                "Languages detected in " + dirNameB + " by language then content type",
                sql, st);

        sql = "select concat(lang_ID1_A, ' -> '," +
                "lang_ID1_B) as \"Language Differences\", count(1) as COUNT " +
                "from comparisons " +
                "where lang_ID1_A <> lang_ID1_B and " +
                "lang_ID1_A is not null and lang_ID1_B is not null "+
                "group by \"Language Differences\" " +
                "order by COUNT desc, (lang_ID1_A || ' -> ' ||  lang_ID1_B)";

        dumpTable(langsDir, "language_differences_in_A_to_B.html",
                "Detected language differences in \"" + dirNameA + "\"->\""+dirNameB+"\"",
                sql, st);

        sql = "select FILE_EXTENSION, " +
                "concat(lang_ID1_A, ' -> ',  lang_ID1_B)"+
                " as \"Language Differences\", " +
                "count(1) as COUNT " +
                "from comparisons " +
                "where lang_ID1_A <> lang_ID1_B and " +
                "lang_ID1_A is not null and lang_ID1_B is not null "+
                "group by FILE_EXTENSION, \"Language Differences\" "+
                "order by FILE_EXTENSION, COUNT desc";

        dumpTable(langsDir, "language_differences_in_A_to_B_by_extension.html",
                "Detected language differences in \"" + dirNameA + "\"->\""+dirNameB+"\"",
                sql, st);

    }

    private void dumpStackTraces(String dirNameA, String dirNameB, File outputDir, Statement st) throws SQLException,
            IOException, SAXException {
        File exceptionsDir = new File(outputDir, "exceptions");
        exceptionsDir.mkdirs();

        String sql = "select SORT_STACK_TRACE_A, count(1) as COUNT "+
                "from comparisons "+
                "where SORT_STACK_TRACE_A is not null "+
                "group by SORT_STACK_TRACE_A "+
                "order by COUNT desc "+
                "LIMIT 50;";
        dumpTable(exceptionsDir, "stack_traces_in_A_overall.html",
                "Top 50 most common stacktraces in " + dirNameA,
                sql, st);

        sql = "select SORT_STACK_TRACE_B, count(1) as COUNT "+
                "from comparisons "+
                "where SORT_STACK_TRACE_B is not null "+
                "group by SORT_STACK_TRACE_B "+
                "order by COUNT desc "+
                "LIMIT 50;";
        dumpTable(exceptionsDir, "stack_traces_in_B_overall.html",
                "Top 50 most common stacktraces in " + dirNameB,
                sql, st);

        sql = "select DETECTED_FILE_EXTENSION_A, " +
                "SORT_STACK_TRACE_A, count(1) as COUNT "+
                "from comparisons "+
                "where SORT_STACK_TRACE_A is not null "+
                "group by DETECTED_FILE_EXTENSION_A, SORT_STACK_TRACE_A "+
                "order by DETECTED_FILE_EXTENSION_A, COUNT desc "+
                "LIMIT 50;";
        dumpTable(exceptionsDir, "stack_traces_in_A_ordered_by_file_extension.html",
                "Top 50 most common stacktraces in " + dirNameA + " ordered by file extension",
                sql, st);

        sql = "select DETECTED_FILE_EXTENSION_B, " +
                "SORT_STACK_TRACE_B, count(1) as COUNT "+
                "from comparisons "+
                "where SORT_STACK_TRACE_B is not null "+
                "group by DETECTED_FILE_EXTENSION_B, SORT_STACK_TRACE_B "+
                "order by DETECTED_FILE_EXTENSION_B, COUNT desc "+
                "LIMIT 50;";
        dumpTable(exceptionsDir, "stack_traces_in_B_ordered_by_file_extension.html",
                "Top 50 most common stacktraces in " + dirNameB + " ordered by file extension",
                sql, st);

        sql = "select DETECTED_FILE_EXTENSION_A, " +
                "SORT_STACK_TRACE_A, count(1) as COUNT "+
                "from comparisons "+
                "where SORT_STACK_TRACE_A is not null "+
                "group by DETECTED_FILE_EXTENSION_A, SORT_STACK_TRACE_A "+
                "order by COUNT desc, DETECTED_FILE_EXTENSION_A "+
                "LIMIT 50;";
        dumpTable(exceptionsDir, "stack_traces_in_A_ordered_by_count.html",
                "Top 50 most common stacktraces in " + dirNameA + " ordered by count",
                sql, st);

        sql = "select DETECTED_FILE_EXTENSION_B, " +
                "SORT_STACK_TRACE_B, count(1) as COUNT "+
                "from comparisons "+
                "where SORT_STACK_TRACE_B is not null "+
                "group by DETECTED_FILE_EXTENSION_B, SORT_STACK_TRACE_B "+
                "order by COUNT desc, DETECTED_FILE_EXTENSION_B "+
                "LIMIT 50;";
        dumpTable(exceptionsDir, "stack_traces_in_B_ordered_by_count.html",
                "Top 50 most common stacktraces in " + dirNameB + " ordered by count",
                sql, st);

    }

    private void dumpDiffContents(String dirNameA, String dirNameB, File outputDir, Statement st)
            throws SQLException, IOException, SAXException {
        File contentDir = new File(outputDir, "content");
        contentDir.mkdirs();

        String sql = "select FILE_EXTENSION, count(1) as COUNT "+
                "from comparisons "+
                "where (JSON_EX_A is null and JSON_EX_B is null and SORT_STACK_TRACE_A is null " +
                "and SORT_STACK_TRACE_B is null) and "+
                "(TOKEN_COUNT_A > 30 or TOKEN_COUNT_B > 30) and "+
                "(overlap < 0.90 or abs(TOKEN_COUNT_A - TOKEN_COUNT_B) > 100) " +
                "group by FILE_EXTENSION "+
                "order by COUNT desc";
        dumpTable(contentDir, "content_diffs_a_and_b_by_extension.html",
                "Files with content differing by more than the overlap threshold in " + dirNameA + " and " + dirNameB,
                sql, st);

        sql = "select FILE_PATH, FILE_EXTENSION as \"File Extension\", DETECTED_FILE_EXTENSION_A " +
                "as \"Detected File Extension A\", " +
                "DETECTED_FILE_EXTENSION_B as \"Detected File Extension B\", " +
                "TOKEN_COUNT_A as \"Token Count A\", " +
                "TOKEN_COUNT_B as \"Token Count B\", " +
                "(TOKEN_COUNT_B-TOKEN_COUNT_A) as \"Token Count B - Token Count A\", "+
                "DEC_PATTERN(OVERLAP, '#.###') as Overlap, "+
                "TOP_10_MORE_IN_A as \"Top 10 Tokens with Higher Counts in A\", "+
                "TOP_10_MORE_IN_B as \"Top 10 Tokens with Higher Counts in B\" "+
                "from comparisons "+
                "where (JSON_EX_A is null and JSON_EX_B is null and SORT_STACK_TRACE_A is null " +
                "and SORT_STACK_TRACE_B is null) and "+
                "(TOKEN_COUNT_A > 30 or TOKEN_COUNT_B > 30) and "+
                "(overlap < 0.90 or abs(TOKEN_COUNT_A - TOKEN_COUNT_B) > 100) " +
                "order by FILE_EXTENSION, " +
                "overlap desc," +
                "abs(TOKEN_COUNT_A-TOKEN_COUNT_B) desc";
        dumpTable(contentDir, "content_diffs_a_and_b_per_file.html",
                "Files with content differing by more than the overlap threshold in " + dirNameA + " and " + dirNameB,
                sql, st);
    }

    private void dumpMimes(String dirNameA, String dirNameB, File outputDir, Statement st) throws SQLException, IOException, SAXException {
        File mimesDir = new File(outputDir, "mimes");
        mimesDir.mkdirs();

        //first a/b alone
        String sql = "select DETECTED_CONTENT_TYPE_A, count(1) as COUNT " +
                "from comparisons " +
                "group by DETECTED_CONTENT_TYPE_A " +
                "order by COUNT desc ";

        dumpTable(mimesDir, "mime_types_in_A.html",
                "Mime types in \"" + dirNameA+"\"",
                sql, st);

        //first a/b alone
        sql = "select DETECTED_CONTENT_TYPE_B, count(1) as COUNT " +
                "from comparisons " +
                "group by DETECTED_CONTENT_TYPE_B " +
                "order by COUNT desc ";

        dumpTable(mimesDir, "mime_types_in_B.html",
                "Mime types in \"" + dirNameB+"\"",
                sql, st);

        sql = "select DETECTED_FILE_EXTENSION_A, count(1) as COUNT " +
                "from comparisons " +
                "group by DETECTED_FILE_EXTENSION_A " +
                "order by COUNT desc ";

        dumpTable(mimesDir, "detected_extension_in_A.html",
                "Detected extensions in \"" + dirNameA+"\"",
                sql, st);

        sql = "select DETECTED_FILE_EXTENSION_B, count(1) as COUNT " +
                "from comparisons " +
                "group by DETECTED_FILE_EXTENSION_B " +
                "order by COUNT desc ";

        dumpTable(mimesDir, "detected_extension_in_B.html",
                "Detected extensions in \"" + dirNameB+"\"",
                sql, st);

        //now look for misleading extensions
        sql = "select concat(FILE_EXTENSION, '->', DETECTED_FILE_EXTENSION_A) as" +
                "\"Actual file extension->Detected file extension\"," +
                "   count(1) as COUNT " +
                "from comparisons " +
                "where FILE_EXTENSION is not null and "+
                    "JSON_EX_A is null and SORT_STACK_TRACE_A is null "+
                    "and FILE_EXTENSION <> DETECTED_FILE_EXTENSION_A "+
                "group by \"Actual file extension->Detected file extension\" " +
                "order by COUNT desc ";

        dumpTable(mimesDir, "extension_diff_from_A.html",
                "Actual file extensions vs. \"Detected extensions\" in \"" + dirNameA+"\"",
                sql, st);

        sql = "select concat(FILE_EXTENSION, '->', DETECTED_FILE_EXTENSION_B)" +
                " as \"Actual file extension->Detected file extension\"," +
                "   count(1) as COUNT " +
                "from comparisons " +
                "where FILE_EXTENSION is not null and "+
                "JSON_EX_B is null and SORT_STACK_TRACE_B is null "+
                "and FILE_EXTENSION <> DETECTED_FILE_EXTENSION_B "+
                "group by \"Actual file extension->Detected file extension\" " +
                "order by COUNT desc ";

        dumpTable(mimesDir, "extension_diff_from_B.html",
                "Actual file extensions vs. \"Detected extensions\" in \"" + dirNameB+"\"",
                sql, st);

        //now for the diffs
        sql = "select concat(DETECTED_CONTENT_TYPE_A, ' -> ',  " +
                "DETECTED_CONTENT_TYPE_B) as \"Differences in Detected Mimes A->B\", count(1) as COUNT " +
                "from comparisons " +
                "where DETECTED_CONTENT_TYPE_A <> DETECTED_CONTENT_TYPE_B and " +
                "DETECTED_CONTENT_TYPE_A is not null and DETECTED_CONTENT_TYPE_B is not null "+
                "group by \"Differences in Detected Mimes A->B\" order by COUNT desc ";

        dumpTable(mimesDir, "mime_diffs_A_to_B.html",
                "Mimes that changed from " + dirNameA + " -> " + dirNameB,
                sql, st);

    }

    private void dumpAttachments(String dirNameA, String dirNameB, File outputDir, Statement st) throws SQLException, IOException, SAXException {
        File exceptionsDir = new File(outputDir, "attachments");
        exceptionsDir.mkdirs();

        String sql = "SELECT comparisons.FILE_EXTENSION, sum(ifnull(NUM_ATTACHMENTS_A, 0)) "+
            "as \"Number of attachments total\", " +
                extensions_total_table +".NUM_FILES as \"Total number of files\", "+
                "DEC_PATTERN( " +
                "(1.0*sum(ifnull(NUM_ATTACHMENTS_A, 0))/"+extensions_total_table+".NUM_FILES)"+
                ", '###,###.#')" +
                "as \"Average number of attachments per file\" "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "group by comparisons.FILE_EXTENSION " +
                "having \"Total number of files\" > 0 "+
                "order by \"Total number of files\" desc;";
        dumpTable(exceptionsDir, "total_attachments_in_A_by_extension.html",
                "Total attachments in \"" + dirNameA + "\" by file extension",
                sql, st);

        sql = "SELECT comparisons.FILE_EXTENSION, sum(ifnull(NUM_ATTACHMENTS_B, 0)) " +
                "as \"Number of attachments total\", " +
                extensions_total_table +".NUM_FILES as TOTAL_FILES, "+
                "DEC_PATTERN("+
                "(1.0*sum(ifnull(NUM_ATTACHMENTS_B, 0))/"+extensions_total_table+".NUM_FILES)"+
                ", '###,###.##')"+
                " as \"Average number of attachments per file\" "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "group by comparisons.FILE_EXTENSION " +
                "having \"Number of attachments total\" > 0 "+
                "order by \"Number of attachments total\" desc;";
        dumpTable(exceptionsDir, "total_attachments_in_B_by_extension.html",
                "Total attachments in \"" + dirNameB + "\" by file extension",
                sql, st);

        //now by detected type for A
        sql = "SELECT comparisons.DETECTED_CONTENT_TYPE_A, sum(ifnull(NUM_ATTACHMENTS_A, 0)) " +
                "as \"Number of attachments total\", " +
                detected_types_A_table +".NUM_FILES as TOTAL_FILES, "+
                "DEC_PATTERN(" +
                "(1.0*sum(ifnull(NUM_ATTACHMENTS_A, 0))/"+detected_types_A_table+".NUM_FILES)"+
                ", '###,###.##')"+
                " as \"Average number of attachments per file\" "+
                "from comparisons " +
                "left outer join "+detected_types_A_table+" on comparisons.DETECTED_CONTENT_TYPE_A="+detected_types_A_table+".DETECTED_CONTENT_TYPE_A "+
                "group by comparisons.DETECTED_CONTENT_TYPE_A " +
                "having \"Number of attachments total\" > 0 "+
                "order by \"Number of attachments total\" desc;";
        dumpTable(exceptionsDir, "total_attachments_in_A_by_detected_type.html",
                "Total attachments in \"" + dirNameA + "\" by detected content type",
                sql, st);

        //now by detected type for B
        sql = "SELECT comparisons.DETECTED_CONTENT_TYPE_B, sum(ifnull(NUM_ATTACHMENTS_B, 0)) " +
                "as \"Number of attachments total\", " +
                detected_types_B_table +".NUM_FILES as TOTAL_FILES, "+
                "DEC_PATTERN(" +
                "(1.0*sum(ifnull(NUM_ATTACHMENTS_B, 0))/"+detected_types_B_table+".NUM_FILES)"+
                ", '###,###.##')"+
                " as \"Average number of attachments per file\" "+
                "from comparisons " +
                "left outer join "+detected_types_B_table+" on comparisons.DETECTED_CONTENT_TYPE_B="+
                    detected_types_B_table+".DETECTED_CONTENT_TYPE_B "+
                "group by comparisons.DETECTED_CONTENT_TYPE_B " +
                "having \"Number of attachments total\" > 0 "+
                "order by \"Number of attachments total\" desc;";
        dumpTable(exceptionsDir, "total_attachments_in_B_by_detected_type.html",
                "Total attachments in \"" + dirNameB + "\" by detected content type",
                sql, st);

        //now go for differences

        sql = "SELECT FILE_EXTENSION, count(1) as COUNT " +
                "from comparisons " +
                "where NUM_ATTACHMENTS_A > NUM_ATTACHMENTS_B and " +
                "JSON_EX_A is null and JSON_EX_B is null and " +
                "SORT_STACK_TRACE_A is null and SORT_STACK_TRACE_B is null "+
                "group by FILE_EXTENSION " +
                "order by COUNT desc;";

        dumpTable(exceptionsDir, "more_attachments_A_than_B.html",
                "Files with more attachments in \"" + dirNameA + "\" than in \"" + dirNameB+"\"",
                sql, st);

        sql = "SELECT FILE_EXTENSION, count(1) as COUNT " +
                "from comparisons " +
                "where NUM_ATTACHMENTS_A < NUM_ATTACHMENTS_B and " +
                "JSON_EX_A is null and JSON_EX_B is null and " +
                "SORT_STACK_TRACE_A is null and SORT_STACK_TRACE_B is null "+
                "group by FILE_EXTENSION " +
                "order by COUNT desc;";

        dumpTable(exceptionsDir, "more_attachments_B_than_A.html",
                "Files with more attachments in \"" + dirNameB + "\" than in \"" + dirNameA+"\"",
                sql, st);

    }

    private void dumpExceptions(String dirNameA, String dirNameB, File outputDir, Statement st)
            throws SQLException, IOException, SAXException {
        File exceptionsDir = new File(outputDir, "exceptions");
        exceptionsDir.mkdirs();

        //dump a and b alone
        //json first
        String sql = "SELECT comparisons.File_Extension, count(1) as COUNT, " +
                extensions_total_table +".NUM_FILES "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "where JSON_EX_A <> ''" +
                "group by "+extensions_total_table+".FILE_EXTENSION " +
                "order by COUNT desc;";

        dumpTable(exceptionsDir, "json_exceptions_in_A_by_extension.html",
                "JSON Exceptions in \"" + dirNameA + "\"",
                sql, st);

        sql = "SELECT comparisons.File_Extension, count(1) as COUNT, " +
                extensions_total_table +".NUM_FILES "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "where JSON_EX_B <> ''" +
                "group by "+extensions_total_table+".FILE_EXTENSION " +
                "order by COUNT desc;";

        dumpTable(exceptionsDir, "json_exceptions_in_B_by_extension.html",
                "JSON Exceptions in \"" + dirNameB + "\"",
                sql, st);

        //now caught exceptions
        sql = "SELECT comparisons.File_Extension, " +
                extensions_total_table +".NUM_FILES as \"Number of files\", "+
                "count(1) as \"Number of files with exceptions\", " +
                "DEC_PATTERN(" +
                "(100.0*ifNull(count(1), 0)/"+extensions_total_table+".NUM_FILES)"+
                ", '#.##') "+
                "as \"Percentage of files with exceptions\" "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "where SORT_STACK_TRACE_A is not null " +
                "group by comparisons.FILE_EXTENSION "+
                        "order by (1.0*ifNull(count(1), 0)/"+extensions_total_table+".NUM_FILES) desc;";

        dumpTable(exceptionsDir, "exceptions_in_A_by_extension.html",
                "Exceptions in \"" + dirNameA + "\"",
                sql, st);

        sql = "SELECT comparisons.File_Extension, " +
                extensions_total_table +".NUM_FILES as \"Number of files\", "+
                "count(1) as \"Number of files with exceptions\", " +
                "DEC_PATTERN( "+
                "(100.0*ifNull(count(1), 0)/"+extensions_total_table+".NUM_FILES)"+
                ", '#.##') "+
                " as \"Percentage of files with exceptions\" "+
                "from comparisons " +
                "left outer join "+extensions_total_table+" on comparisons.FILE_EXTENSION="+extensions_total_table+".FILE_EXTENSION "+
                "where SORT_STACK_TRACE_B is not null " +
                "group by comparisons.FILE_EXTENSION " +
                "order by (1.0*ifNull(count(1), 0)/"+extensions_total_table+".NUM_FILES) desc;";
        dumpTable(exceptionsDir, "exceptions_in_B_by_extension.html",
                "Exceptions in \"" + dirNameB + "\"",
                sql, st);


        sql = "SELECT comparisons.DETECTED_CONTENT_TYPE_A as \"Detected content type\", " +
                detected_types_A_table +".NUM_FILES as \"Number of files\", "+
                "count(1) as \"Number of files with exceptions\", " +
                "DEC_PATTERN("+
                "(100.0*ifNull(count(1), 0)/"+detected_types_A_table+".NUM_FILES) "+
                ", '#.##') "+
                " as \"Percentage of files with exceptions\" "+
                "from comparisons " +
                "left outer join "+detected_types_A_table+" on comparisons.DETECTED_CONTENT_TYPE_A ="
                    +detected_types_A_table+".DETECTED_CONTENT_TYPE_A "+
                "where SORT_STACK_TRACE_A is not null " +
                "group by comparisons.DETECTED_CONTENT_TYPE_A " +
                "order by (1.0*ifNull(count(1), 0)/"+detected_types_A_table+".NUM_FILES) desc;";

        dumpTable(exceptionsDir, "exceptions_in_A_by_detected_type.html",
                "Exceptions in \"" + dirNameA + "\"",
                sql, st);


        sql = "SELECT comparisons.DETECTED_CONTENT_TYPE_B as \"Detected content type\", " +
                detected_types_B_table +".NUM_FILES as \"Number of files\", "+
                "count(1) as \"Number of files with exceptions\", " +
                "DEC_PATTERN("+
                "(100.0*ifNull(count(1), 0)/"+detected_types_B_table+".NUM_FILES)"+
                ", '#.##')"+
                " as \"Percentage of files with exceptions\" "+
                "from comparisons " +
                "left outer join "+detected_types_B_table+" on comparisons.DETECTED_CONTENT_TYPE_B ="
                +detected_types_B_table+".DETECTED_CONTENT_TYPE_B "+
                "where SORT_STACK_TRACE_B is not null " +
                "group by comparisons.DETECTED_CONTENT_TYPE_B " +
                "order by (1.0*ifNull(count(1), 0)/"+detected_types_B_table+".NUM_FILES) desc;";

        dumpTable(exceptionsDir, "exceptions_in_B_by_detected_type.html",
                "Exceptions in \"" + dirNameB + "\"",
                sql, st);

        //now for the comparisons
        //json exceptions in b not a = "new" json exceptions
        sql = "SELECT File_Extension, count(1) as COUNT " +
                "from comparisons " +
                "where (JSON_EX_A = '' or JSON_EX_A is null) and " +
                "(JSON_EX_B <> '')" +
                "group by FILE_EXTENSION " +
                "order by COUNT desc;";

        dumpTable(exceptionsDir, "json_exceptions_in_B_not_A.html",
                "JSON Exceptions in " + dirNameB + " but not in " + dirNameA,
                sql, st);

        sql = "SELECT File_Extension, count(1) as COUNT " +
                "from comparisons " +
                "where JSON_EX_A <> '' and JSON_EX_B is null " +
                "group by FILE_EXTENSION " +
                "order by COUNT desc";
        dumpTable(exceptionsDir, "json_exceptions_in_A_not_B.html",
                "JSON Exceptions in " + dirNameA + " but not in " + dirNameB,
                sql, st);

        sql = "SELECT File_Extension, count(1) as COUNT " +
                "from comparisons " +
                "where SORT_STACK_TRACE_A is null and SORT_STACK_TRACE_B <> '' and " +
                "JSON_EX_A is null and JSON_EX_B is null " +
                "group by FILE_EXTENSION " +
                "order by COUNT desc";
        dumpTable(exceptionsDir, "exceptions_in_B_not_A.html",
                "Exceptions in \"" + dirNameB + "\" but not in \"" + dirNameA+"\"",
                sql, st);

        sql = "SELECT File_Extension, count(1) as COUNT " +
                "from comparisons " +
                "where SORT_STACK_TRACE_A <> '' and SORT_STACK_TRACE_B is null and " +
                "JSON_EX_A is null and JSON_EX_B is null " +
                "group by FILE_EXTENSION " +
                "order by COUNT desc";

        dumpTable(exceptionsDir, "exceptions_in_A_not_B.html",
                "Exceptions in \"" + dirNameA + "\" but not in \"" + dirNameB+"\"",
                sql, st);

        dumpExceptionComparisonTable(dirNameA, dirNameB, exceptionsDir, st);

    }

    private void dumpExceptionComparisonTable(String dirNameA, String dirNameB, File exceptionsDir, Statement st)
            throws SQLException, IOException, SAXException {

        String sql = "select "+extensions_total_table+".FILE_EXTENSION as \"File Extension\", " +
                "NUM_FILES as \"Number of Files\", " +
                "ifNull(NUM_CAUGHT_EXCEPTIONS_A, 0) as \"Number of Exceptions in A\", " +
                "ifNull(NUM_CAUGHT_EXCEPTIONS_B, 0) as \"Number of Exceptions in B\", " +
                "(ifNull(NUM_CAUGHT_EXCEPTIONS_B, 0) - ifNull(NUM_CAUGHT_EXCEPTIONS_A, 0)) as " +
                "   \"Number of Differences A to B\", " +
                "DEC_PATTERN("+
                    "(100.0*ifNull("+exceptions_A_table+".NUM_CAUGHT_EXCEPTIONS_A, 0)/NUM_FILES)"+
                ", '#.##') "+
                "as \"Percent Exceptions in A\", " +
                "DEC_PATTERN("+
                    "(100.0*ifNull("+exceptions_B_table+".NUM_CAUGHT_EXCEPTIONS_B, 0)/NUM_FILES)"+
                ", '#.##')"+
                " as \"Percent Exceptions in B\" " +
                "from " +extensions_total_table+" "+
                "left join " + exceptions_A_table +" on "+extensions_total_table+".file_extension ="+exceptions_A_table+".file_extension " +
                "left join " + exceptions_B_table + " on "+extensions_total_table+".file_extension ="+exceptions_B_table+".file_extension " +
                "where (ifNull(NUM_CAUGHT_EXCEPTIONS_B, 0) - ifNull(NUM_CAUGHT_EXCEPTIONS_A, 0)) <> 0 "+
                "order by abs(ifNull(NUM_CAUGHT_EXCEPTIONS_B, 0) - ifNull(NUM_CAUGHT_EXCEPTIONS_A, 0)) desc;";
        dumpTable(exceptionsDir, "differences_in_exception_counts.html",
                "Differences in exceptions in \"" + dirNameA + "\" vs. \"" + dirNameB+"\"",
                sql,
                st);
    }

    private String[] getDirNames(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        String sql = "select DIR_NAME_A, DIR_NAME_B from "+ JDBCTableWriter.PAIR_NAMES_TABLE;
        ResultSet rs = st.executeQuery(sql);
        rs.next();
        String[] ret = new String[2];
        ret[0] = rs.getString(1);
        ret[1] = rs.getString(2);
        return ret;
    }

    private void dumpTable(File outputDir, String fileName, String header, String sql,
                           Statement st) throws IOException, SQLException, SAXException {
        ResultSet rs = st.executeQuery(sql);
        OutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(outputDir, fileName));
            ContentHandler wrapped = new ToHTMLContentHandler();
            XHTMLContentHandler handler = new XHTMLContentHandler(wrapped, new Metadata());
            handler.startDocument();
            handler.startElement("body");
            handler.startElement("h1");
            handler.characters(header);
            handler.endElement("h1");
            handler.startElement("table", "border", "2");
            ResultSetMetaData rsMeta = rs.getMetaData();

            handler.startElement("tr");
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                handler.startElement("th");
                handler.characters(rsMeta.getColumnName(i));
                handler.endElement("th");
            }
            handler.endElement("tr");
            int rows = 0;
            while (rs.next()) {
                handler.startElement("tr");
                for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                    handler.startElement("td");
                    handler.startElement("pre");
                    handler.characters(rs.getString(i));
                    handler.endElement("pre");
                    handler.endElement("td");
                }
                handler.endElement("tr");
                rows++;
            }
            handler.endElement("table");
            handler.startElement("br");
            handler.endElement("br");
            handler.startElement("br");
            handler.endElement("br");
            handler.startElement("br");
            handler.endElement("br");
            handler.startElement("br");
            handler.endElement("br");
            handler.characters(sql);
            handler.endElement("body");
            handler.endDocument();

            //if no results, replace handler
            if (rows == 0) {
                handler = buildNoResultsHandler(header, sql);
            }
            IOUtils.write(handler.toString(), fos, org.apache.tika.io.IOUtils.UTF_8);

        } finally {
            if (fos != null) {
                fos.flush();
                IOUtils.closeQuietly(fos);
            }
            rs.close();
        }
    }

    private XHTMLContentHandler buildNoResultsHandler(String header, String sql) throws SAXException {
        ContentHandler wrapped = new ToHTMLContentHandler();
        XHTMLContentHandler handler = new XHTMLContentHandler(wrapped, new Metadata());
        handler.startDocument();
        handler.startElement("body");
        handler.startElement("h1");
        handler.characters(header);
        handler.endElement("h1");
        handler.characters("There were no results for this query.");
        handler.startElement("br");
        handler.endElement("br");
        handler.startElement("br");
        handler.endElement("br");
        handler.startElement("br");
        handler.endElement("br");
        handler.startElement("br");
        handler.endElement("br");
        handler.characters(sql);
        handler.endElement("body");
        handler.endDocument();
        return handler;
    }

    public static void main(String[] args) throws IOException, SQLException, SAXException {
        File dbFile = new File(args[0]);
        File outputDir = new File(args[1]);


        if (! outputDir.isDirectory()) {
            outputDir.mkdirs();
        }
        if (!outputDir.isDirectory()) {
            throw new RuntimeException("Couldn't make output directory!");
        }
        FileComparerResultDumper dumper = new FileComparerResultDumper();
        dumper.execute(dbFile, outputDir);
    }

}
