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
package org.apache.tika.eval.db;

public enum Cols {
    //container table
    CONTAINER_ID,
    FILE_PATH,
    EXTRACT_FILE_LENGTH,

    EXTRACT_FILE_LENGTH_A, //for comparisons
    EXTRACT_FILE_LENGTH_B,

    //profile table
    ID,
    LENGTH,
    FILE_NAME,
    FILE_EXTENSION,
    ELAPSED_TIME_MILLIS,
    NUM_METADATA_VALUES,
    IS_EMBEDDED,
    EMBEDDED_FILE_PATH,
    MIME_TYPE_ID,
    MD5,
    NUM_ATTACHMENTS,
    HAS_CONTENT,

    //content
    CONTENT_LENGTH,
    UNIQUE_TOKEN_COUNT,
    TOKEN_COUNT,
    COMMON_WORDS_LANG, //which language was used for the common words metric?
    NUM_COMMON_WORDS,
    TOP_N_WORDS,
    NUM_EN_STOPS_TOP_N,
    LANG_ID_1,
    LANG_ID_PROB_1,
    LANG_ID_2,
    LANG_ID_PROB_2,
    TOKEN_ENTROPY_RATE,
    TOKEN_LENGTH_SUM,
    TOKEN_LENGTH_MEAN,
    TOKEN_LENGTH_STD_DEV,
    UNICODE_CHAR_BLOCKS,

    //content comparisons
    TOP_10_UNIQUE_TOKEN_DIFFS_A,
    TOP_10_UNIQUE_TOKEN_DIFFS_B,
    TOP_10_MORE_IN_A,
    TOP_10_MORE_IN_B,
    OVERLAP,
    DICE_COEFFICIENT,

    //errors
    PARSE_ERROR_TYPE_ID,

    PARSE_ERROR_DESCRIPTION,
    PARSE_EXCEPTION_DESCRIPTION,

    EXTRACT_ERROR_TYPE_ID,
    EXTRACT_ERROR_DESCRIPTION,


    //exceptions
    ORIG_STACK_TRACE,
    SORT_STACK_TRACE,
    PARSE_EXCEPTION_TYPE_ID,


    MIME_STRING,//string representation of mime type

    DIR_NAME_A,//for comparisons in REF_PAIR_NAMES
    DIR_NAME_B
    }

