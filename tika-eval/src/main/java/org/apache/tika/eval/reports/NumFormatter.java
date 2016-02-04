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

package org.apache.tika.eval.reports;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

class NumFormatter implements ColFormatter {

    private final String formatString;

    NumFormatter(String formatString) {
        this.formatString = formatString;
    }



    @Override
    public String getString(int i, ResultSet rs) throws SQLException {
        int type = rs.getMetaData().getColumnType(i);
        DecimalFormat format = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.ROOT));

        switch (type) {
            case Types.INTEGER :
                return format.format((double)rs.getInt(i)).trim();
            case Types.DOUBLE :
                return format.format(rs.getDouble(i)).trim();
            case Types.FLOAT :
                return format.format(rs.getFloat(i)).trim();
            case Types.DECIMAL :
                return format.format(rs.getBigDecimal(i).doubleValue()).trim();
            case Types.BIGINT :
                return format.format(rs.getBigDecimal(i).doubleValue()).trim();
            default :
                throw new IllegalArgumentException("Number formatter can't handle SQL type:"+type);
        }
    }

    @Override
    public String getFormatString() {
        return formatString;
    }
}
