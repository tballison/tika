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

package org.apache.tika.metadata.values;

import org.apache.tika.metadata.MetadataValue;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.PropertyTypeException;

/**
 * Integer metadata value
 */
public class IntMetadataValue extends MetadataValue {

    private final Integer i;

    public IntMetadataValue(final String val) {
        super(val);
        Integer tmp = null;
        try {
            tmp = Integer.valueOf(val);
        } catch(NumberFormatException e) {
        }
        i = tmp;
        resetStringVal((i == null) ? val : Integer.toString(i));

    }

    public IntMetadataValue(final Integer i) {
        super(Integer.toString(i));
        this.i = i;
        resetStringVal((i == null) ? null : Integer.toString(i));
    }

    @Override
    public boolean isAllowed(Property property) {
        if(property.getPrimaryProperty().getPropertyType() != Property.PropertyType.SIMPLE) {
            throw new PropertyTypeException(Property.PropertyType.SIMPLE, property.getPrimaryProperty().getPropertyType());
        }
        if(property.getPrimaryProperty().getValueType() != Property.ValueType.INTEGER) {
            throw new PropertyTypeException(Property.ValueType.INTEGER, property.getPrimaryProperty().getValueType());
        }
        return true;
    }

    public Integer getInt() {
        return i;
    }

    @Override
    public String toString() {
        return "IntMetadataValue{" +
                "i=" + i +
                ", intString='" + getString() + '\'' +
                '}';
    }
}
