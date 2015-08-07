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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MultimediaTracksValue extends MetadataValue {
    public static String MM_TRACK_DUMMY_STRING = "MultimediaTracksValue...too complex to stringify meaningfully";
    private Map<String, MediaTrack> tracks = new HashMap<>();

    public MultimediaTracksValue() {
        //set a dummy for the initialization,
        //use resetString after object has been built...need to clean this up!
        super(MM_TRACK_DUMMY_STRING);
    }

    public void addMediaTrack(String key, MediaTrack track) {
        tracks.put(key, track);
    }

    public Set<String> getTracks() {
        return Collections.unmodifiableSet(tracks.keySet());
    }

    public MediaTrack getTrack(String key) {
        return tracks.get(key);
    }
}
