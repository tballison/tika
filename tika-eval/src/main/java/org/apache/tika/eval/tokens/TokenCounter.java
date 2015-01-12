package org.apache.tika.eval.tokens;

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

import java.util.Collection;

public abstract class TokenCounter {
    int tokenCount = 0;
    int uniqueTokenCount = 0;

    public abstract void increment(String s);

    /**
     * Returns the tokens in this counter.
     * WARNING: In BasicFileComparer, this can return tokens that don't actually exist in this
     * counter!  Make sure to check getCount() > 0.
     * @return
     */
    public abstract Collection<String> getTokens();

    public abstract int getCount(String token);

    /**
     *
     * @param currentCount the current count of the token in this counter
     */
    public void incrementOverallCounts(int currentCount) {
        if (currentCount == 0){
            uniqueTokenCount++;
        }
        tokenCount++;
    }

    public int getUniqueTokenCount() {
        return uniqueTokenCount;
    }

    public int getTokenCount() {
        return tokenCount;
    }

}
