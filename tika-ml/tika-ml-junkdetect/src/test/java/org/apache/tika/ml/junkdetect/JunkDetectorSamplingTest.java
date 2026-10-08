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
package org.apache.tika.ml.junkdetect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.tika.quality.TextQualityScore;

public class JunkDetectorSamplingTest {

    private static final String CLEAN_PARAGRAPH = "The quick brown fox jumps over the lazy dog. "
            + "Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump! ";

    private static JunkDetector detector;

    @BeforeAll
    static void loadModel() throws Exception {
        detector = JunkDetector.loadFromClasspath();
    }

    private static String repeat(String s, int minChars) {
        StringBuilder sb = new StringBuilder(minChars + s.length());
        while (sb.length() < minChars) {
            sb.append(s);
        }
        return sb.toString();
    }

    private static String garbage(int len, long seed) {
        byte[] bytes = new byte[len];
        new Random(seed).nextBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (0x80 | (bytes[i] & 0x7F));
        }
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    @Test
    void shortTextIsNotSampled() {
        String text = repeat(CLEAN_PARAGRAPH, 1000);
        assertSame(text, JunkDetector.sample(text, text.length()));
    }

    @Test
    void sampleCoversTheWholeDocument() {
        String text = repeat(CLEAN_PARAGRAPH, 400_000);
        String sampled = JunkDetector.sample(text, 10_000);
        assertTrue(sampled.length() <= 10_000 + 2 * JunkDetector.SAMPLE_WINDOWS, "len=" + sampled.length());
        assertTrue(sampled.startsWith(text.substring(0, 100)), "first window starts at the start");
        assertTrue(sampled.endsWith(text.substring(text.length() - 100)), "last window ends at the end");
        assertTrue(sampled.indexOf("\n\n") > 0, "windows are separated");
        // every edge the sampler cut lands on a word boundary
        String[] windows = sampled.split("\n\n");
        assertEquals(JunkDetector.SAMPLE_WINDOWS, windows.length);
        for (int i = 0; i < windows.length; i++) {
            String window = windows[i];
            assertFalse(window.isEmpty());
            if (i > 0) {
                assertFalse(Character.isWhitespace(window.charAt(0)), "window " + i + " start");
            }
            if (i < windows.length - 1) {
                assertFalse(Character.isWhitespace(window.charAt(window.length() - 1)), "window " + i + " end");
            }
        }
    }

    @Test
    void sampleNeverSplitsSurrogatePairs() {
        // no whitespace anywhere, so the edges can only snap on surrogates
        String text = repeat("𝔘𝔙", 50_001);
        for (int cap : new int[]{7, 101, 1_003, 10_005}) {
            String sampled = JunkDetector.sample(text, cap);
            for (int i = 0; i < sampled.length(); i++) {
                char c = sampled.charAt(i);
                if (Character.isHighSurrogate(c)) {
                    assertTrue(i + 1 < sampled.length() && Character.isLowSurrogate(sampled.charAt(i + 1)),
                            "unpaired high surrogate at " + i + " cap=" + cap);
                    i++;
                } else {
                    assertFalse(Character.isLowSurrogate(c), "unpaired low surrogate at " + i + " cap=" + cap);
                }
            }
        }
    }

    @Test
    void sampledScoreTracksFullScore() {
        String text = repeat(CLEAN_PARAGRAPH, 600_000);
        TextQualityScore full = detector.withMaxScoredChars(Integer.MAX_VALUE).score(text);
        TextQualityScore sampled = detector.score(text);
        assertEquals(full.getZScore(), sampled.getZScore(), 0.25f, "full=" + full + " sampled=" + sampled);
        assertTrue(sampled.getZScore() > 0, "clean text stays clean: " + sampled);
    }

    @Test
    void corruptionInTheTailStillLowersTheScore() {
        String clean = repeat(CLEAN_PARAGRAPH, 600_000);
        String tailJunk = clean.substring(0, 500_000) + garbage(100_000, 7);
        float cleanZ = detector.score(clean).getZScore();
        float tailJunkZ = detector.score(tailJunk).getZScore();
        assertTrue(tailJunkZ < cleanZ - 1.0f, "clean=" + cleanZ + " tailJunk=" + tailJunkZ);
    }

    @Test
    void withMaxScoredCharsReturnsConfiguredCopy() {
        assertEquals(JunkDetector.DEFAULT_MAX_SCORED_CHARS, detector.getMaxScoredChars());
        JunkDetector small = detector.withMaxScoredChars(500);
        assertEquals(500, small.getMaxScoredChars());
        assertEquals(JunkDetector.DEFAULT_MAX_SCORED_CHARS, detector.getMaxScoredChars());
        assertThrows(IllegalArgumentException.class, () -> detector.withMaxScoredChars(0));
    }
}
