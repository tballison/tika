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

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.tika.metadata.Property.PropertyType;
import org.apache.tika.metadata.values.DateMetadataValue;
import org.apache.tika.metadata.values.DoubleMetadataValue;
import org.apache.tika.metadata.values.IntMetadataValue;

/**
 * A multi-valued metadata container.
 */
public class Metadata implements CreativeCommons, Geographic, HttpHeaders,
        Message, MSOffice, ClimateForcast, TIFF, TikaMetadataKeys, TikaMimeKeys,
        Serializable {

    /** Serial version UID */
    private static final long serialVersionUID = 5623926545693153182L;

    /**
     * A map of all metadata attributes.
     */
    private Map<String, MetadataValue[]> metadata = null;

    /**
     * The common delimiter used between the namespace abbreviation and the property name
     */
    public static final String NAMESPACE_PREFIX_DELIMITER = ":";

    /** @deprecated use TikaCoreProperties#FORMAT */
    public static final String FORMAT = "format";
    /** @deprecated use TikaCoreProperties#IDENTIFIER */
    public static final String IDENTIFIER = "identifier";
    /** @deprecated use TikaCoreProperties#MODIFIED */
    public static final String MODIFIED = "modified";
    /** @deprecated use TikaCoreProperties#CONTRIBUTOR */
    public static final String CONTRIBUTOR = "contributor";
    /** @deprecated use TikaCoreProperties#COVERAGE */
    public static final String COVERAGE = "coverage";
    /** @deprecated use TikaCoreProperties#CREATOR */
    public static final String CREATOR = "creator";
    /** @deprecated use TikaCoreProperties#CREATED */
    public static final Property DATE = Property.internalDate("date");
    /** @deprecated use TikaCoreProperties#DESCRIPTION */
    public static final String DESCRIPTION = "description";
    /** @deprecated use TikaCoreProperties#LANGUAGE */
    public static final String LANGUAGE = "language";
    /** @deprecated use TikaCoreProperties#PUBLISHER */
    public static final String PUBLISHER = "publisher";
    /** @deprecated use TikaCoreProperties#RELATION */
    public static final String RELATION = "relation";
    /** @deprecated use TikaCoreProperties#RIGHTS */
    public static final String RIGHTS = "rights";
    /** @deprecated use TikaCoreProperties#SOURCE */
    public static final String SOURCE = "source";
    /** @deprecated use TikaCoreProperties#KEYWORDS */
    public static final String SUBJECT = "subject";
    /** @deprecated use TikaCoreProperties#TITLE */
    public static final String TITLE = "title";
    /** @deprecated use TikaCoreProperties#TYPE */
    public static final String TYPE = "type";



    /**
     * Constructs a new, empty metadata.
     */
    public Metadata() {
        metadata = new HashMap<String, MetadataValue[]>();
    }

    /**
     * Returns true if named value is multivalued.
     * 
     * @param property
     *          metadata property
     * @return true is named value is multivalued, false if single value or null
     */
    public boolean isMultiValued(final Property property) {
        return metadata.get(property.getName()) != null && metadata.get(property.getName()).length > 1;
    }
    
    /**
     * Returns true if named value is multivalued.
     * 
     * @param name
     *          name of metadata
     * @return true is named value is multivalued, false if single value or null
     */
    public boolean isMultiValued(final String name) {
        return metadata.get(name) != null && metadata.get(name).length > 1;
    }

    /**
     * Returns an array of the names contained in the metadata.
     * 
     * @return Metadata names
     */
    public String[] names() {
        return metadata.keySet().toArray(new String[metadata.keySet().size()]);
    }

    /**
     * Get the String value associated to a metadata name. If many values are assiociated
     * to the specified name, then the first one is returned.
     * @deprecated To be removed in Tika 2.0.  Use {@link #getMetadataValue(String)}.
     * @param name
     *          of the metadata.
     * @return the value associated to the specified metadata name.
     */
    @Deprecated
    public String get(final String name) {
        MetadataValue[] values = metadata.get(name);
        if (values == null) {
            return null;
        } else {
            return values[0].getString();
        }
    }

    /**
     * Get the value associated to a metadata name. If many values are assiociated
     * to the specified name, then the first one is returned.
     *
     * @param property
     *          of the metadata.
     * @return the value associated to the specified property or null if not found.
     */
    public MetadataValue getMetadataValue(final Property property) {
        return getMetadataValue(property.getName());
    }


    /**
     * Get the value associated to a metadata name. If many values are assiociated
     * to the specified name, then the first one is returned.
     *
     * @param name
     *          of the metadata.
     * @return the value associated to the specified metadata name or null if not found.
     */
    public MetadataValue getMetadataValue(final String name) {
        MetadataValue[] values = metadata.get(name);
        if (values == null) {
            return null;
        } else {
            return values[0];
        }
    }

    /**
     * Returns the value (if any) of the identified metadata property.
     *
     * @since Apache Tika 0.7
     * @param property property definition
     * @return property value, or <code>null</code> if the property is not set
     */
    public String get(Property property) {
        return get(property.getName());
    }
    
    /**
     * Returns the value of the identified Integer based metadata property.
     * 
     * @since Apache Tika 0.8
     * @deprecated To be removed in Tika 2.0.  Use {@link #getMetadataValue(Property)}
     * @param property simple integer property definition
     * @return property value as a Integer, or <code>null</code> if the property is not set, or not a valid Integer
     */
    @Deprecated
    public Integer getInt(Property property) {
        if(property.getPrimaryProperty().getPropertyType() != Property.PropertyType.SIMPLE) {
            return null;
        }
        if(property.getPrimaryProperty().getValueType() != Property.ValueType.INTEGER) {
            return null;
        }
        
        String v = get(property);
        if(v == null) {
            return null;
        }
        try {
            return Integer.valueOf(v);
        } catch(NumberFormatException e) {
            return null;
        }
    }

    /**
     * Returns the value of the identified Date based metadata property.
     * 
     * @since Apache Tika 0.8
     * @deprecated To be removed in Tika 2.0.  Use {@link #getMetadataValue(String)}.
     * @param property simple date property definition
     * @return property value as a Date, or <code>null</code> if the property is not set, or not a valid Date
     */
    @Deprecated
    public Date getDate(Property property) {
        if(property.getPrimaryProperty().getPropertyType() != Property.PropertyType.SIMPLE) {
            return null;
        }
        if(property.getPrimaryProperty().getValueType() != Property.ValueType.DATE) {
            return null;
        }
        
        String v = get(property);
        DateMetadataValue dmv = new DateMetadataValue(v);
        return dmv.getDate();
    }
    
    /**
     * Get the values associated to a metadata name.
     *
     * @deprecated To be removed in Tika 2.0.  Use {@link #getMetadataValues(Property)}
     *
     * @param property
     *          of the metadata.
     * @return the values associated to a metadata name.
     */
    @Deprecated
    public String[] getValues(final Property property) {
        return _getValues(property.getName());
    }

    /**
     * Get the String values associated to a metadata name.
     * @deprecated To be removed in Tika 2.0.  Use {@link #getMetadataValues(String)}.
     *
     * @param name
     *          of the metadata.
     * @return the values associated to a metadata name.
     */
    @Deprecated
    public String[] getValues(final String name) {
        return _getValues(name);
    }

    /**
     * Get the values associated to a metadata name.
     *
     *
     * @param property
     *          of the metadata.
     * @return the values associated to a metadata name.
     */
    public MetadataValue[] getMetadataValues(final Property property) {
        return _getMetadataValues(property.getName());
    }

    /**
     * Get the {@link MetadataValue}s associated to a metadata name.
     *
     * @param name
     *          of the metadata.
     * @return the values associated to a metadata name.
     */
    public MetadataValue[] getMetadataValues(final String name) {
        return _getMetadataValues(name);
    }


    private MetadataValue[] _getMetadataValues(final String name) {
        MetadataValue[] values = metadata.get(name);
        if (values == null) {
            return new MetadataValue[0];
        }
        return values;
    }

    @Deprecated
    private String[] _getValues(final String name) {
        MetadataValue[] values = _getMetadataValues(name);
        if (values.length == 0) {
            return new String[0];
        }
        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            stringValues[i] = values[i].getString();
        }
        return stringValues;
    }
    
    private MetadataValue[] appendedValues(MetadataValue[] values, final MetadataValue value) {
        MetadataValue[] newValues = new MetadataValue[values.length + 1];
        System.arraycopy(values, 0, newValues, 0, values.length);
        newValues[newValues.length - 1] = value;
        return newValues;
    }

    /**
     * Add a metadata name/value mapping. Add the specified value to the list of
     * values associated to the specified metadata name.
     *
     * @deprecated This will be removed in Tika 2.0.
     *    Use {@link #add(String, MetadataValue)}, or consider using
     *    {@link #add(Property, MetadataValue)} if possible.
     *
     * @param name
     *          the metadata name.
     * @param value
     *          the metadata value.
     */
    @Deprecated
    public void add(final String name, final String value) {
        MetadataValue[] values = metadata.get(name);
        if (values == null) {
            set(name, new MetadataValue(value));
        } else {
            metadata.put(name, appendedValues(values, new MetadataValue(value)));
        }
    }

    /**
     * Add a metadata name/value mapping. Add the specified value to the list of
     * values associated to the specified metadata name.
     *
     * @deprecated This will be removed in Tika 2.0.
     *    Use {@link #add(String, MetadataValue)}, or consider using
     *    {@link #add(Property, MetadataValue)} if possible.
     *
     * @param name
     *          the metadata name.
     * @param value
     *          the metadata value.
     */
    public void add(final String name, final MetadataValue value) {
        MetadataValue[] values = metadata.get(name);
        if (values == null) {
            set(name, value);
        } else {
            metadata.put(name, appendedValues(values, value));
        }

    }
    /**
     * Add a metadata property/value String mapping. Add the specified
     * String value to the list of
     * values associated to the specified metadata property.
     *
     * @deprecated This will be removed in Tika 2.0.
     * Use {@link #add(Property, MetadataValue)} instead.
     *
     * @param property
     *          the metadata property.
     * @param value
     *          the metadata value.
     */
    @Deprecated
    public void add(final Property property, final String value) {
        MetadataValue[] values = metadata.get(property.getName());
        if (values == null) {
            set(property, new MetadataValue(value));
        } else {
             if (property.isMultiValuePermitted()) {
                 set(property, appendedValues(values, new MetadataValue(value)));
             } else {
                 throw new PropertyTypeException(property.getPropertyType());
             }
        }
    }

    /**
     * Add a metadata property/value mapping. Add the specified value to the list of
     * values associated to the specified metadata property.
     *
     * @param property
     *          the metadata property.
     * @param value
     *          the metadata value.
     */
    public void add(final Property property, final MetadataValue value) {
        if (! value.isAllowed(property)) {
            throw new PropertyTypeException(value.getClass() + " is not compatible with "
                    +property.toString());
        }
        MetadataValue[] values = metadata.get(property.getName());
        if (values == null) {
            set(property, value);
        } else {
            if (property.isMultiValuePermitted()) {
                set(property, appendedValues(values, value));
            } else {
                throw new PropertyTypeException(property.getPropertyType());
            }
        }
    }
    /**
     * Copy All key-value String pairs from properties.
     * 
     * @param properties
     *          properties to copy from
     */
    @SuppressWarnings("unchecked")
    public void setAll(Properties properties) {
        Enumeration<String> names =
            (Enumeration<String>) properties.propertyNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            metadata.put(name, new MetadataValue[]{ new MetadataValue(properties.getProperty(name)) });
        }
    }

    /**
     * Set metadata name/value via Strings. Associate the specified value to the specified
     * metadata name. If some previous values were associated to this name,
     * they are removed. If the given value is <code>null</code>, then the
     * metadata entry is removed.
     *
     * @deprecated This will be removed in Tika 2.0.
     * Use {@link #set(Property, MetadataValue)}  or {@link #set(String, MetadataValue)} instead.
     *
     * @param name the metadata name.
     * @param value  the metadata value, or <code>null</code>
     */
    @Deprecated
    public void set(String name, String value) {
        if (value == null) {
            set(name, (MetadataValue) null);
        } else {
            set(name, new MetadataValue(value));
        }
    }

    /**
     * Set metadata name/value. Associate the specified value to the specified
     * metadata name. If some previous values were associated to this name,
     * they are removed. If the given value is <code>null</code>, then the
     * metadata entry is removed.
     *
     * @param name the metadata name.
     * @param value  the metadata value, or <code>null</code>
     */
    public void set(String name, MetadataValue value) {
        if (value != null) {
            metadata.put(name, new MetadataValue[]{value});
        } else {
            metadata.remove(name);
        }
    }

    /**
     * Sets the String value of the identified metadata property.
     *
     * @since Apache Tika 0.7
     * @deprecated To be removed in Tika 2.0.  Use {@link #set(Property, MetadataValue[])} instead.
     * @param property property definition
     * @param value    property value
     */
    @Deprecated
    public void set(Property property, String value) {
        set(property, new MetadataValue(value));
    }

    /**
     * Sets the value of the identified metadata property.
     *
     * @since Apache Tika 0.7
     * @param property property definition
     * @param value    property value
     */
    public void set(Property property, MetadataValue value) {
        if (property == null) {
            throw new NullPointerException("property must not be null");
        }
        if (! value.isAllowed(property)) {
            throw new PropertyTypeException(value.getClass().getName() +
                " is not compatible with property: "+property.getName());
        }
        if (property.getPropertyType() == PropertyType.COMPOSITE) {
            set(property.getPrimaryProperty(), value);
            if (property.getSecondaryExtractProperties() != null) {
                for (Property secondaryExtractProperty : property.getSecondaryExtractProperties()) {
                    set(secondaryExtractProperty, value);
                }
            }
        } else {
            set(property.getName(), value);
        }
    }
    
    /**
     * Sets the values of the identified metadata property.
     *
     * @since Apache Tika 1.2
     * @deprecated To be removed in Tika 2.0.  Use {@link #set(Property, MetadataValue[])} instead.
     * @param property property definition
     * @param values    property values
     */
    @Deprecated
    public void set(Property property, String[] values) {
        if (property == null) {
            throw new NullPointerException("property must not be null");
        }
        MetadataValue[] metadataValues = new MetadataValue[values.length];
        for (int i = 0; i < values.length; i++) {
            metadataValues[i] = new MetadataValue(values[i]);
        }
        set(property, metadataValues);
    }

    /**
     * Sets the values of the identified metadata property.
     *
     * @since Apache Tika 1.2
     * @param property property definition
     * @param values    property values
     */
    public void set(Property property, MetadataValue[] values) {
        if (property == null) {
            throw new NullPointerException("property must not be null");
        }
        if (property.getPropertyType() == PropertyType.COMPOSITE) {
            set(property.getPrimaryProperty(), values);
            if (property.getSecondaryExtractProperties() != null) {
                for (Property secondaryExtractProperty : property.getSecondaryExtractProperties()) {
                    set(secondaryExtractProperty, values);
                }
            }
        } else {
            for (MetadataValue v : values) {
                if (! v.isAllowed(property)) {
                    throw new PropertyTypeException(v.getClass().getName() +
                            " is not compatible with property: "+property.getName());
                }
            }
            metadata.put(property.getName(), values);
        }
    }

    /**
     * Sets the integer value of the identified metadata property.
     *
     * @since Apache Tika 0.8
     * @deprecated To be removed in Tika 2.0.  Use {@link #set(Property, MetadataValue)} instead.
     * @param property simple integer property definition
     * @param value    property value
     */
    @Deprecated
    public void set(Property property, int value) {
        set(property, new IntMetadataValue(value));
    }

    /**
     * Sets the real or rational value of the identified metadata property.
     *
     * @since Apache Tika 0.8
     * @deprecated To be removed in Tika 2.0.  Use {@link #set(Property, MetadataValue)} instead.
     * @param property simple real or simple rational property definition
     * @param value    property value
     */
    @Deprecated
    public void set(Property property, double value) {
        set(property, new DoubleMetadataValue(value));
    }

    /**
     * Sets the date value of the identified metadata property.
     *
     * @since Apache Tika 0.8
     * @deprecated To be removed in Tika 2.0.  Use {@link #set(Property, MetadataValue)} instead.
     * @param property simple integer property definition
     * @param date     property value
     */
    @Deprecated
    public void set(Property property, Date date) {
        set(property, new DateMetadataValue(date));
    }

    /**
     * Sets the date value of the identified metadata property.
     *
     * @since Apache Tika 0.8
     * @deprecated To be removed in Tika 2.0.  Use {@link #set(Property, MetadataValue)} instead.
     * @param property simple integer property definition
     * @param date     property value
     */
    @Deprecated
    public void set(Property property, Calendar date) {
        //TODO: check to see if this is valid or if we need a CalendarMetadataValue
        //that'd allow us not to lose Calendar info...probably better
        set(property, new DateMetadataValue(date.getTime()));
    }

    /**
     * Remove a metadata and all its associated values.
     * 
     * @param name
     *          metadata name to remove
     */
    public void remove(String name) {
        metadata.remove(name);
    }

    /**
     * Returns the number of metadata names in this metadata.
     * 
     * @return number of metadata names
     */
    public int size() {
        return metadata.size();
    }

    public boolean equals(Object o) {

        if (o == null) {
            return false;
        }

        Metadata other = null;
        try {
            other = (Metadata) o;
        } catch (ClassCastException cce) {
            return false;
        }

        if (other.size() != size()) {
            return false;
        }

        String[] names = names();
        for (int i = 0; i < names.length; i++) {
            MetadataValue[] otherValues = other._getMetadataValues(names[i]);
            MetadataValue[] thisValues = _getMetadataValues(names[i]);
            if (otherValues.length != thisValues.length) {
                return false;
            }
            for (int j = 0; j < otherValues.length; j++) {
                if (!otherValues[j].equals(thisValues[j])) {
                    return false;
                }
            }
        }
        return true;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        String[] names = names();
        for (int i = 0; i < names.length; i++) {
            MetadataValue[] values = _getMetadataValues(names[i]);
            for (int j = 0; j < values.length; j++) {
                buf.append(names[i]).append("=").append(values[j].getString()).append(" ");
            }
        }
        return buf.toString();
    }

}
