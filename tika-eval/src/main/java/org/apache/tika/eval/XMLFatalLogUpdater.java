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
import java.sql.ResultSet;
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
    private Statement statement;

    public static void main(String[] args) throws Exception {

        XMLFatalLogUpdater writer = new XMLFatalLogUpdater();
        File xmlLogFileA = new File(args[0]);
        File xmlLogFileB = new File(args[1]);
        File dbFile = new File(args[2]);
        writer.execute(xmlLogFileA, dbFile);
        writer.execute(xmlLogFileB, dbFile);
    }

    private void execute(File xmlLogFile, File dbFile) throws Exception {
        DBUtil dbUtil = new H2Util(dbFile);
        Connection connection = dbUtil.getConnection();
        statement = connection.createStatement();
        XMLLogReader reader = new XMLLogReader();
        InputStream is = null;
        try {
            is = new FileInputStream(xmlLogFile);
            reader.read(is, new FatalMsgUpdater());
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


        @Override
        public void handleMsg(Level level, String xml) throws SQLException, IOException {
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
                                update(resourceId,
                                        AbstractProfiler.ERROR_TYPE.TIMEOUT);

                            } else if ("oom".equals(reader.getLocalName())) {
                                resourceId = reader.getAttributeValue("", "resourceId");
                                update(resourceId, AbstractProfiler.ERROR_TYPE.OOM);
                            }
                            break;
                    }
                }
                reader.close();
            } catch (XMLStreamException e) {
                throw new IOExceptionWithCause(e);
            }
        }

        private void update(String resourceId, AbstractProfiler.ERROR_TYPE type) throws SQLException {

            int containerId = -1;
            String sql = "SELECT " + AbstractProfiler.CONTAINER_HEADERS.CONTAINER_ID.name() +
                    " from " +AbstractProfiler.CONTAINERS_TABLE +
                    " where " + AbstractProfiler.CONTAINER_HEADERS.FILE_PATH.name() +
                    " ="+resourceId;
            ResultSet rs = statement.executeQuery(sql);
            int resultCount = 0;
            while (rs.next()) {
                containerId = rs.getInt(1);
                resultCount++;
            }
            rs.close();

            if (containerId < 0) {
                sql = "SELECT MAX("+ AbstractProfiler.CONTAINER_HEADERS.CONTAINER_ID.name()+
                        ") from "+AbstractProfiler.CONTAINERS_TABLE;
                rs = statement.executeQuery(sql);
                while (rs.next()) {
                    containerId = rs.getInt(1);
                }
                rs.close();
                if (containerId < 0) {
                    containerId = 0;
                } else {
                    containerId++;
                }

            }

            sql = "SELECT count(1) from "+AbstractProfiler.ERRORS_TABLE +
                    " where "+AbstractProfiler.ERROR_HEADERS.CONTAINER_ID.name()+
                    " = "+containerId;
            rs = statement.executeQuery(sql);

            int hitCount = 0;
            while (rs.next()) {
                hitCount++;
            }

            if (hitCount > 0) {
                sql = "UPDATE " + AbstractProfiler.ERRORS_TABLE +
                        " SET " + AbstractProfiler.ERROR_HEADERS.ERROR_TYPE.name() +
                        " = " + type.ordinal() +
                        " where "+AbstractProfiler.ERROR_HEADERS.CONTAINER_ID.name()+
                        "="+containerId;

            } else {
                sql = "INSERT INTO " + AbstractProfiler.ERRORS_TABLE +
                        " values (" + containerId+", "+type.ordinal()+","+
                        AbstractProfiler.FALSE+");";

            }
            statement.executeUpdate(sql);
        }


    }

}
