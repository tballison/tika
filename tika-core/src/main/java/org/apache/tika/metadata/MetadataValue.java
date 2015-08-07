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
package org.apache.tika.metadata;

/**
 * Base class for a metadata value.  This is a container for a String
 * but is built to be extended for all other metadata values.
 */
public class MetadataValue {

    protected String stringValue;

    public MetadataValue(String s) {
        this.stringValue = s;
    }

    protected void resetStringVal(String s) {
        this.stringValue = s;
    }
    /**
     *
     * @return String value of this metadata value object, can be null
     */
    public String getString() {
        return stringValue;
    }

    /**
     *
     * @param property
     * @return whether or not this metadata value is compatible with this
     * property
     * @throws PropertyTypeException
     */
    public boolean isAllowed(Property property) {
        //TODO: need to make this more stringent
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetadataValue that = (MetadataValue) o;

        return !(stringValue != null ? !stringValue.equals(that.stringValue) : that.stringValue != null);

    }

    @Override
    public int hashCode() {
        return stringValue != null ? stringValue.hashCode() : 0;
    }
}
