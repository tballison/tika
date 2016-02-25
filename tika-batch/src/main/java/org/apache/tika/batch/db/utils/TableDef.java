package org.apache.tika.batch.db.utils;

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

import java.util.LinkedList;
import java.util.List;

public class TableDef {

    String tableName;
    List<ColDef> columns;

    public TableDef(String tableName, ColDef ... colDefs) {
        this.tableName = tableName;
        columns = new LinkedList<>();
        for (ColDef colDef : colDefs) {
            columns.add(colDef);
        }
    }

    public String getCreateSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName);
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            ColDef colDef = columns.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(colDef.getName()).append(" ").append(colDef.getSQLDef());
        }
        sb.append(");");
        return sb.toString();
    }

    public String getTableName() {
        return tableName;
    }
}
