package org.apache.tika.eval.io;

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

import java.io.IOException;
import java.util.Map;

/**
 * Common interface for writing output.  Currently relying on ThreadSafeCSVWrapper,
 * but it would be better to be able to specify a db.
 */
public interface TableWriter {
    /**
     * if the writer needs to write headers, call this
     */
    public void writeHeaders() throws IOException;

    public void init() throws IOException;

    public void writeRow(Map<String, String> data) throws IOException;

    /**
     * give hint to stop processing, no more input available
     */
    public void shutdown();

    /**
     * now actually close all resources
     * @throws IOException
     */
    public void close() throws IOException;
}
