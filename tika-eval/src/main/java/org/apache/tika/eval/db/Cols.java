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
    ELAPSED_TIME_MILLIS,
    NUM_METADATA_VALUES,
    IS_EMBEDDED,
    FILE_EXTENSION,
    EMBEDDED_FILE_PATH,
    //content
    NUM_UNIQUE_TOKENS,
    MIME,
    LENGTH,
    MD5,
    NUM_ATTACHMENTS,
    UNIQUE_TOKEN_COUNT,
    TOKEN_COUNT,
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

    //content comparisons
    TOP_10_UNIQUE_TOKEN_DIFFS_A,
    TOP_10_UNIQUE_TOKEN_DIFFS_B,
    TOP_10_MORE_IN_A,
    TOP_10_MORE_IN_B,
    OVERLAP,
    DICE_COEFFICIENT,

    //errors
    ERROR_ID,
    JSON_EX,

    //exceptions
    ORIG_STACK_TRACE,
    SORT_STACK_TRACE,
    EXCEPTION_ID,

    }

