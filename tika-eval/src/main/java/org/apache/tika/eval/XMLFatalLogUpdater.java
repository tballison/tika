package org.apache.tika.eval;

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

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Level;
import org.apache.tika.eval.db.DBUtil;
import org.apache.tika.eval.db.H2Util;
import org.apache.tika.eval.io.XMLLogMsgHandler;
import org.apache.tika.eval.io.XMLLogReader;
import org.apache.tika.io.IOExceptionWithCause;
import org.apache.tika.io.IOUtils;

/**
 * This is a very task specific class that reads a log file and updates
 * the "comparisons" table.  It should not be run in a multithreaded environment.
 */
class XMLFatalLogUpdater {
    private DBUtil dbUtil = new H2Util();
    private Statement statement;
    private String tableName;

    public static void main(String[] args) throws Exception {

        XMLFatalLogUpdater writer = new XMLFatalLogUpdater();
        File xmlLogFileA = new File(args[0]);
        File xmlLogFileB = new File(args[1]);
        File dbFile = new File(args[2]);
        writer.execute(xmlLogFileA, dbFile, "comparisons", "_A");
        writer.execute(xmlLogFileB, dbFile, "comparisons", "_B");
    }

    private void execute(File xmlLogFile, File dbFile, String tableName, String columnSuffix) throws Exception {
        Connection connection = dbUtil.getConnection(dbFile);
        statement = connection.createStatement();
        this.tableName = tableName;
        XMLLogReader reader = new XMLLogReader();
        InputStream is = null;
        try {
            is = new FileInputStream(xmlLogFile);
            reader.read(is, new FatalMsgUpdater(AbstractProfiler.HEADERS.FILE_PATH.name(),
                    AbstractProfiler.HEADERS.OOM_ERROR.name()+columnSuffix,
                    AbstractProfiler.HEADERS.TIMEOUT_EXCEPTION.name()+columnSuffix));
        } catch (IOException e) {
            throw new RuntimeException("Doh!");
        } finally {
            IOUtils.closeQuietly(is);
            try {
                connection.commit();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close db connection!", e);
            }
        }
    }

    private class FatalMsgUpdater implements XMLLogMsgHandler {

        private final String keyField;
        private final String oomField;
        private final String timeoutField;

        FatalMsgUpdater(String keyField, String oomField, String timeoutField) {
            this.keyField = keyField;
            this.oomField = oomField;
            this.timeoutField = timeoutField;
        }

        @Override
        public void handleMsg(Level level, String xml) throws IOException {
            if (! level.equals(Level.FATAL)) {
                return;
            }
            XMLStreamReader reader = null;
            try {
                reader = XMLInputFactory.newInstance().createXMLStreamReader(new StringReader(xml));
            } catch (XMLStreamException e) {
                throw new IOExceptionWithCause(e);
            }
            String type = null;
            String resourceId = null;
            try {
                while (reader.hasNext() && type == null && resourceId == null) {
                    reader.next();
                    switch (reader.getEventType()) {
                        case XMLStreamConstants.START_ELEMENT:
                            if ("timeout".equals(reader.getLocalName())) {
                                resourceId = reader.getAttributeValue("", "resourceId");
                                update(resourceId, timeoutField);
                            } else if ("oom".equals(reader.getLocalName())) {
                                resourceId = reader.getAttributeValue("", "resourceId");
                                update(resourceId, oomField);
                            }
                            break;
                    }
                }
                reader.close();
            } catch (XMLStreamException e) {
                throw new IOExceptionWithCause(e);
            }
        }

        private void update(String resourceId, String errorField) {
            String sql = "UPDATE "+tableName+" set "+errorField+"=true "+
                    "where "+keyField+"="+resourceId;
            try {
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

}
