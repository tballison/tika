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

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class ColDef {

    public final static int NO_PRECISION = Integer.MIN_VALUE;

    private final static Map<Integer, String> typeMap;

    static {
        typeMap = new HashMap<>();

        for (Field field : Types.class.getFields()) {
            try {
                typeMap.put((Integer)field.get(null), field.getName());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    };

    final String name;
    final int type;
    final int precision;
    final String constraints;

    public ColDef(String name, int type) {
        this(name, type, NO_PRECISION, null);
    }

    public ColDef(String name, int type, String constraints) {
        this(name, type, NO_PRECISION, constraints);
    }

    public ColDef(String name, int type, int precision) {
        this(name, type, precision, null);
    }

    public ColDef(String name, int type, int precision, String constraints) {
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.constraints = constraints;
    }

    public String getSQLDef() {
        StringBuilder sb = new StringBuilder();
        sb.append(typeMap.get(type));
        if (precision != NO_PRECISION) {
            sb.append("(").append(Integer.toString(precision)).append(")");
        }
        if (constraints != null) {
            sb.append(" ").append(constraints);
        }
        return sb.toString();
    }

    public String getName() {
        return name;
    }
}
