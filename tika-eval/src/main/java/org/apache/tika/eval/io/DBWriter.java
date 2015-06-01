package org.apache.tika.eval.io;

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.tika.eval.db.ColInfo;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.io.IOExceptionWithCause;

/**
 * This is still in its early stages.  The idea is to
 * get something working with h2 and then add to that
 * as necessary.
 *
 * Beware, this deletes the db file with each initialization.
 */
public class DBWriter implements IDBWriter {
    
    private static Logger logger = Logger.getLogger(DBWriter.class);
    private final AtomicLong insertedRows = new AtomicLong();
    private final Long commitEveryX = 1000L;

    private final Map<String, Map<String, ColInfo>> tableInfo;
    private final Connection conn;
    private final DBUtil dbUtil;
    //<tableName, preparedStatement>
    private final Map<String, PreparedStatement> inserts = new HashMap<String, PreparedStatement>();

    public DBWriter(Map<String, Map<String, ColInfo>> tableInfo, DBUtil dbUtil) throws IOException {

        this.conn = dbUtil.getConnection();
        this.tableInfo = tableInfo;
        this.dbUtil = dbUtil;
        for (Map.Entry<String, Map<String, ColInfo>> table : tableInfo.entrySet()) {
            List<String> colNames = new ArrayList<String>();
            colNames.addAll(table.getValue().keySet());
            try {
                PreparedStatement st = createPreparedInsert(table.getKey(), colNames);
                inserts.put(table.getKey(), st);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private PreparedStatement createPreparedInsert(String tableName, List<String> colNames) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append("(");
        int i = 0;
        for (String c : colNames) {
            if (i++ > 0) {
                sb.append(", ");
            }
            sb.append(c);
        }
        sb.append(") ");

        sb.append("VALUES");
        sb.append("(");
        for (int j = 0; j < i; j++) {
            if (j > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        sb.append(")");

        return conn.prepareStatement(sb.toString());
    }


    @Override
    public void writeRow(String tableName, Map<String, String> data) throws IOException {
        try {
            PreparedStatement p = inserts.get(tableName);
            if (p == null) {
                throw new RuntimeException("Failed to create prepared statement for: "+tableName);
            }
            dbUtil.insert(p, tableInfo.get(tableName), data);
            long rows = insertedRows.incrementAndGet();
            if (rows % commitEveryX == 0) {
                logger.info("writer is committing after "+ rows + " rows");
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            conn.commit();
        } catch (SQLException e){
            throw new IOExceptionWithCause(e);
        }
        try{
            conn.close();
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
        }

    }
}
