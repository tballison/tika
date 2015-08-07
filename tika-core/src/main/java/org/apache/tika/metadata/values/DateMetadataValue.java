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

import static org.apache.tika.utils.DateUtils.MIDDAY;
import static org.apache.tika.utils.DateUtils.UTC;
import static org.apache.tika.utils.DateUtils.formatDate;

import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.tika.metadata.MetadataValue;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.PropertyTypeException;

public class DateMetadataValue extends MetadataValue {

    private final Date date;

    /**
     * Some parsers will have the date as a ISO-8601 string
     *  already, and will set that into the Metadata object.
     * So we can return Date objects for these, this is the
     *  list (in preference order) of the various ISO-8601
     *  variants that we try when processing a date based
     *  property.
     */
    private static final DateFormat[] iso8601InputFormats = new DateFormat[] {
            // yyyy-mm-ddThh...
            createDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", UTC),   // UTC/Zulu
            createDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", null),    // With timezone
            createDateFormat("yyyy-MM-dd'T'HH:mm:ss", null),     // Without timezone
            // yyyy-mm-dd hh...
            createDateFormat("yyyy-MM-dd' 'HH:mm:ss'Z'", UTC),   // UTC/Zulu
            createDateFormat("yyyy-MM-dd' 'HH:mm:ssZ", null),    // With timezone
            createDateFormat("yyyy-MM-dd' 'HH:mm:ss", null),     // Without timezone
            // Date without time, set to Midday UTC
            createDateFormat("yyyy-MM-dd", MIDDAY),              // Normal date format
            createDateFormat("yyyy:MM:dd", MIDDAY),              // Image (IPTC/EXIF) format
    };

    private static DateFormat createDateFormat(String format, TimeZone timezone) {
        SimpleDateFormat sdf =
                new SimpleDateFormat(format, new DateFormatSymbols(Locale.US));
        if (timezone != null) {
            sdf.setTimeZone(timezone);
        }
        return sdf;
    }

    /**
     * Parses the given date string. This method is synchronized to prevent
     * concurrent access to the thread-unsafe date formats.
     *
     * @see <a href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
     * @param date date string
     * @return parsed date, or <code>null</code> if the date can't be parsed
     */
    private static synchronized Date parseDate(String date) {
        // Java doesn't like timezones in the form ss+hh:mm
        // It only likes the hhmm form, without the colon
        int n = date.length();
        if (date.charAt(n - 3) == ':'
                && (date.charAt(n - 6) == '+' || date.charAt(n - 6) == '-')) {
            date = date.substring(0, n - 3) + date.substring(n - 2);
        }

        // Try several different ISO-8601 variants
        for (DateFormat format : iso8601InputFormats) {
            try {
                return format.parse(date);
            } catch (ParseException ignore) {
            }
        }
        return null;
    }

    public DateMetadataValue(final String val) {
        super(val);
        date = parseDate(val);
        if (date != null) {
            resetStringVal(formatDate(date));
        }
    }

    public DateMetadataValue(final Date date) {
        super(formatDate(date));
        //defensive copy
        this.date = new Date(date.getTime());
    }

    @Override
    public boolean isAllowed(Property property) {
        if(property.getPrimaryProperty().getPropertyType() != Property.PropertyType.SIMPLE) {
            throw new PropertyTypeException(Property.PropertyType.SIMPLE, property.getPrimaryProperty().getPropertyType());
        }
        if(property.getPrimaryProperty().getValueType() != Property.ValueType.DATE) {
            throw new PropertyTypeException(Property.ValueType.DATE, property.getPrimaryProperty().getValueType());
        }
        return true;
    }

    public Date getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "DateMetadataValue{" +
                "date=" + date +
                '}';
    }
}
