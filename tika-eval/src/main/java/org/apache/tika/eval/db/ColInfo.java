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

import java.sql.Types;

public class ColInfo {
    private final int dbColOffset;//offset starting at 1
    private final int type;
    private final Integer precision;

    public ColInfo(int dbColOffset, int type) {
        this(dbColOffset, type, null);
    }

    public ColInfo(int dbColOffset, int type, Integer precision) {
        this.dbColOffset = dbColOffset;
        this.type = type;
        this.precision = precision;
    }

    public int getDBColOffset() {
        return dbColOffset;
    }

    public int getJavaColOffset() {
        return dbColOffset-1;
    }

    public int getType() {
        return type;
    }

    /**
     * Gets the precision.  This can be null!
     * @return precision or null
     */
    public Integer getPrecision() {
        return precision;
    }

    public String getSqlDef() {
        if (type == Types.VARCHAR){
            return "VARCHAR("+precision+")";
        }
        switch (type) {
            case Types.FLOAT :
                return "FLOAT";
            case Types.DOUBLE :
                return "DOUBLE";
            case Types.BLOB :
                return "BLOB";
            case Types.INTEGER :
                return "INTEGER";
            case Types.BIGINT :
                return "BIGINT";
            case Types.BOOLEAN :
                return "BOOLEAN";
        }
        throw new UnsupportedOperationException("Don't yet recognize a type for: "+type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColInfo colInfo = (ColInfo) o;

        if (dbColOffset != colInfo.dbColOffset) return false;
        if (type != colInfo.type) return false;
        if (precision != null ? !precision.equals(colInfo.precision) : colInfo.precision != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dbColOffset;
        result = 31 * result + type;
        result = 31 * result + (precision != null ? precision.hashCode() : 0);
        return result;
    }
}
