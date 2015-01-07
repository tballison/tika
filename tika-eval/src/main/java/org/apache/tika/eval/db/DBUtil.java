package org.apache.tika.eval.db;
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

public class DBUtil {

    public static int insert(PreparedStatement insertStatement,
                              Map<String, ColInfo> columns,
                              Map<String, String> data) throws SQLException {

        //clear parameters before setting
        insertStatement.clearParameters();
        for (Map.Entry<String, ColInfo> e : columns.entrySet()) {
            updateInsertStatement(insertStatement, e.getValue(), data.get(e.getKey()));
        }
        return insertStatement.executeUpdate();
    }

    public static void updateInsertStatement(PreparedStatement st,
                                                ColInfo colInfo, String value ) throws SQLException {
        if (value == null) {
            return;
        }
        System.out.println("DBCOL OFFSET: " + colInfo.getDBColOffset() + " : " + colInfo.getType() + " : " + colInfo.getSqlDef() + " : " + value + " = "+Types.VARCHAR);
        switch(colInfo.getType()) {
            case Types.VARCHAR :
                st.setString(colInfo.getDBColOffset(), value);
                break;
            case Types.DOUBLE :
                st.setDouble(colInfo.getDBColOffset(), Double.parseDouble(value));
                break;
            case Types.FLOAT :
                st.setDouble(colInfo.getDBColOffset(), Float.parseFloat(value));
                break;
            case Types.INTEGER :
                st.setDouble(colInfo.getDBColOffset(), Integer.parseInt(value));
                break;
            default:
                throw new UnsupportedOperationException("Don't yet support type: "+colInfo.getType());
        }
    }
}
