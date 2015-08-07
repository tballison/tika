package org.apache.tika.metadata.serialization;

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.*;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.MetadataValue;


/**
 * Deserializer for Metadata
 *
 * If overriding this, remember that this is called from a static context.
 * Share state only with great caution.
 */
public class JsonMetadataDeserializer implements JsonDeserializer<Metadata> {

    /**
     * Deserializes a json object (equivalent to: Map<String, String[]>) 
     * into a Metadata object.
     * 
     * @param element to serialize
     * @param type (ignored)
     * @param context (ignored)
     * @return Metadata 
     * @throws JsonParseException if element is not able to be parsed
     */
    @Override
    public Metadata deserialize(JsonElement element, Type type,
            JsonDeserializationContext context) throws JsonParseException {

        final JsonObject obj = element.getAsJsonObject();
        Metadata m = new Metadata();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()){
            String key = entry.getKey();
            JsonElement v = entry.getValue();
            if (v.isJsonArray()){
                JsonArray vArr = v.getAsJsonArray();
                Iterator<JsonElement> itr = vArr.iterator();
                while (itr.hasNext()) {
                    JsonElement valueItem = itr.next();
                    m.add(key, buildMetadataValue(valueItem));
                }
            } else {
                m.set(key, buildMetadataValue(v));
            }
        }
        return m;
    }

    private MetadataValue buildMetadataValue(JsonElement v) {

        if (v.isJsonPrimitive()) {
            return new MetadataValue(v.getAsString());
        } else if (v.isJsonObject()) {
            JsonElement el = v.getAsJsonObject().get(JsonMetadataBase.CLASS_KEY);
            if (el == null) {
                throw new IllegalArgumentException("Map must contain key: "+ JsonMetadataBase.CLASS_KEY);
            }
            if (! el.isJsonPrimitive()) {
                throw new IllegalArgumentException("Value must be a primitive, not:"+getType(el));
            }
            String className = el.getAsString();

            el = v.getAsJsonObject().get(JsonMetadataBase.METADATA_VALUE_KEY);
            if (el == null) {
                throw new IllegalArgumentException("Map must contain a value for key: "+
                        JsonMetadataBase.METADATA_VALUE_KEY);
            }
            return buildMetadataValue(className, el);
        }
        throw new IllegalArgumentException("Metadata value must be primitive or jsonObject, not:"+
                getType(v));
    }

    private MetadataValue buildMetadataValue(String className, JsonElement metadataValueJson) {
        //TODO: should we do static caching of c? Benchmark to determine if it matters...
        Class c = null;
        try {
            c = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found", e);
        }

        Object obj = JsonMetadataBase.METADATA_VALUE_GSON.fromJson(metadataValueJson, c);
        if (obj instanceof MetadataValue) {
            return (MetadataValue)obj;
        }
        throw new IllegalArgumentException(className + " must be an instance of MetadataValue!");
    }

    private String getType(JsonElement el) {
        String jType = "";
        if (el.isJsonObject()) {
            jType = "jsonObject";
        } else if (el.isJsonArray()) {
            jType = "jsonArray";
        } else if (el.isJsonNull()) {
            jType = "jsonNull";
        }
        return jType;
    }
}
