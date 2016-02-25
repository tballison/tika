package org.apache.tika.batch.db;
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
import java.sql.Types;

import org.apache.tika.batch.db.utils.ColDef;
import org.apache.tika.batch.db.utils.TableDef;


/**
 * WARNING "auto_increment" is specific to H2
 */
public class Tables {

    final static int MAX_FILE_NAME_LENGTH = 1024;
    final static int MAX_PATH_LENGTH = 1024;

    public static final String ROOTS_TABLE_NAME = "ROOTS";
    public static final String REL_PATHS_TABLE_NAME = "REL_PATHS";
    public static final String FILE_STATUS_TABLE_NAME = "FILE_STATUS";
    public static final String FLAT_TABLE_NAME = "FLAT_TABLE";
    public static final String CRAWLER_INSERTER_STATUS_TABLE_NAME = "CRAWLER_INSERTER_STATUS";
    public static final String FILE_DATA_TABLE_NAME = "FILE_DATA";
    public static final String TIKA_EXCEPTIONS_TABLE_NAME = "TIKA_EXCEPTIONS";

    public static  TableDef ROOTS =
            new TableDef(ROOTS_TABLE_NAME,
                    new ColDef("ROOT_ID", Types.INTEGER, "AUTO_INCREMENT PRIMARY KEY"),
                    new ColDef("ROOT_PATH", Types.VARCHAR, MAX_PATH_LENGTH, "NOT NULL")//UNIQUE
            );

    public static final TableDef REL_PATHS =
            new TableDef(REL_PATHS_TABLE_NAME,
                    new ColDef("PATH_ID", Types.INTEGER, "AUTO_INCREMENT PRIMARY KEY"),
                    new ColDef("ROOT_ID", Types.INTEGER, "NOT NULL"),//FOREIGN KEY
                    new ColDef("REL_PATH", Types.VARCHAR, MAX_PATH_LENGTH, "NOT NULL")
            );

    public static final TableDef FILE_STATUS =
            new TableDef(FILE_STATUS_TABLE_NAME,
                    new ColDef("FILE_ID", Types.INTEGER, "AUTO_INCREMENT PRIMARY KEY"),
                    new ColDef("PATH_ID", Types.INTEGER, "NOT NULL"),//FOREIGN KEY
                    new ColDef("FILE_NAME", Types.VARCHAR, MAX_FILE_NAME_LENGTH, "NOT NULL"),
                    new ColDef("PROCESSING_STATUS", Types.INTEGER, "NOT NULL"),
                    //new ColDef("LAST_UPDATED", Types.TIMESTAMP, "NOT NULL"),
                    new ColDef("PROCESSING_FINISHED", Types.TIMESTAMP)
            );

    public static final TableDef FLAT_TABLE = new TableDef(FLAT_TABLE_NAME,
            new ColDef("FILE_ID", Types.INTEGER, "AUTO_INCREMENT PRIMARY KEY"),
        new ColDef("FILE_PATH", Types.VARCHAR, MAX_PATH_LENGTH+MAX_FILE_NAME_LENGTH),
        new ColDef("PROCESSING_STATUS", Types.INTEGER, "NOT NULL"),
        //new ColDef("LAST_UPDATED", Types.TIMESTAMP, "NOT NULL"),
        new ColDef("PROCESSING_FINISHED", Types.TIMESTAMP)
    );



    public static final TableDef CRAWLER_INSERTER_STATUS =
            new TableDef(CRAWLER_INSERTER_STATUS_TABLE_NAME,
                    new ColDef("STATUS", Types.INTEGER),
                    new ColDef("LAST_UPDATED", Types.TIMESTAMP));


    public static final TableDef FILE_DATA =
            new TableDef(FILE_DATA_TABLE_NAME,
                    new ColDef("FILE_ID", Types.INTEGER, "NOT NULL"),//FOREIGN KEY
                    new ColDef("CONTENT", Types.BLOB, "NOT NULL")
            );

    public static final TableDef TIKA_EXCEPTIONS =
            new TableDef(TIKA_EXCEPTIONS_TABLE_NAME,
                    new ColDef("FILE_ID", Types.INTEGER, "NOT NULL"),//FOREIGN KEY
                    new ColDef("STACK_TRACE", Types.VARCHAR, 1024)
            );
}
