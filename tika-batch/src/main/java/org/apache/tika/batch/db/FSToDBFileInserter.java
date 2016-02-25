package org.apache.tika.batch.db;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.tika.batch.FileResourceCrawler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to crawl file structure and insert files
 * into the db with status of
 * {@link BatchDBConstants.FILE_STATUS#NEEDS_TO_BE_PROCESSED}
 *
 * <p/>
 * This assumes that no other thread is modifying the table!!!
 */
public class FSToDBFileInserter implements Callable<FileInserterStatus> {
    final static Logger logger = LoggerFactory.getLogger(FileResourceCrawler.class.toString());

    final Path root;
    final Connection connection;
    final boolean append;
    PreparedStatement insertInserterStatus;
    PreparedStatement relPathExists;
    PreparedStatement relPathInsert;
    PreparedStatement fileStatusExists;
    PreparedStatement insertFileStatusStatement;

    public FSToDBFileInserter(Path root, Connection connection, boolean append) {
        this.root = root;
        this.connection = connection;
        this.append = append;
    }

    @Override
    public FileInserterStatus call() throws Exception {
        if (! needToAddMore()) {
            logger.info("Don't need to add more; not even starting the crawl");
            return null;
        }
        String sql = "insert into "+Tables.CRAWLER_INSERTER_STATUS_TABLE_NAME + "(?,?)";
        insertInserterStatus = connection.prepareStatement(sql);
        insertFileStatusStatement.clearParameters();
        insertFileStatusStatement.setInt(1, BatchDBConstants.DB_INSERTER_STATUS.STARTED.ordinal());
        insertFileStatusStatement.setTimestamp(2, new Timestamp(Calendar.getInstance().getTime().getTime()));
        insertFileStatusStatement.execute();

        connection.createStatement().execute(sql);
        int rootId = upsertRoot(root);
        sql = "SELECT PATH_ID FROM "+ Tables.REL_PATHS_TABLE_NAME +
                " WHERE REL_PATH=?";

        relPathExists = connection.prepareStatement(sql);

        sql = "INSERT INTO "+Tables.REL_PATHS_TABLE_NAME +
                "(ROOT_ID, REL_PATH) VALUES (?, ?)";

        relPathInsert =
                connection.prepareStatement(sql);

        sql = "SELECT FILE_ID FROM "+Tables.FILE_STATUS_TABLE_NAME+
            " where PATH_ID=? and FILE_NAME=?";

        fileStatusExists =
                connection.prepareStatement(sql);

        //untested postgres
        /*
        sql = "INSERT INTO "+Tables.FILE_STATUS_TABLE_NAME +
                " ( PATH_ID, FILE_NAME, PROCESSING_STATUS) "+
                " VALUES (?,?,?)" +
                " WHERE NOT EXISTS ( SELECT FILE_ID FROM "+Tables.FILE_DATA_TABLE_NAME +
                "   where PATH_ID=? and FILE_NAME=?);";
*/
        /* performance is horrible!!!
        sql = "MERGE INTO "+ Tables.FILE_STATUS_TABLE_NAME +
                " (PATH_ID, FILE_NAME, PROCESSING_STATUS)" +
                " KEY (PATH_ID, FILE_NAME) VALUES (?,?,?)";*/

        sql = "INSERT INTO "+ Tables.FILE_STATUS_TABLE_NAME +
                " (PATH_ID, FILE_NAME, PROCESSING_STATUS)"+
                " VALUES (?,?,?)";
        insertFileStatusStatement =
                connection.prepareStatement(sql);

        addFiles(rootId, root);
        insertFileStatusStatement.clearParameters();
        insertFileStatusStatement.setInt(1, BatchDBConstants.DB_INSERTER_STATUS.FINISHED.ordinal());
        insertFileStatusStatement.setTimestamp(2, new Timestamp(Calendar.getInstance().getTime().getTime()));
        insertFileStatusStatement.execute();

        return new FileInserterStatus();
    }

    private boolean needToAddMore() throws SQLException {
        String sql = "select STATUS from "+
                Tables.CRAWLER_INSERTER_STATUS_TABLE_NAME +
                " order by LAST_UPDATED desc "+
                " LIMIT 1";
        ResultSet rs = connection.createStatement().executeQuery(sql);
        if (rs.next()) {
            int status = rs.getInt(1);
            if (status == BatchDBConstants.DB_INSERTER_STATUS.FINISHED.ordinal()) {
                return false;
            }
        }
        return true;
    }

    private void addFiles(int rootId, Path directory) throws InterruptedException {

        if (directory == null) {
            logger.warn("FSToDBFileInserter asked to process null directory?!");
            return;
        }
        int pathId = -1;
        try {
            pathId = insertPath(rootId, directory);
        } catch (SQLException e) {
            throw new RuntimeException("couldn't add ");
        }
        List<Path> directories = new LinkedList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(directory)){
            for (Path p : ds) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("file adder interrupted");
                }
                if (Files.isDirectory(p)) {
                    directories.add(p);
                } else {
                    tryToInsertFile(pathId, p);
                }
            }
        } catch (IOException e) {
            logger.warn("FSFileAdder couldn't read "+directory.toAbsolutePath() +
                    ": "+e.getMessage());
        }
        for (Path dir : directories) {
            addFiles(rootId, dir);
        }
    }

    private void tryToInsertFile(int pathId, Path p) {
        if (!Files.isReadable(p)) {
            logger.warn("Skipping -- " + p.toAbsolutePath() +
                    " -- file/directory is not readable");
            return;
        }

        String fileName = p.getFileName().toString();
        try {
            insertFileStatusStatement.clearParameters();
            insertFileStatusStatement.setInt(1, pathId);
            insertFileStatusStatement.setString(2, fileName);
            insertFileStatusStatement.setInt(3, BatchDBConstants.FILE_STATUS.NEEDS_TO_BE_PROCESSED.ordinal());
            boolean ex = insertFileStatusStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private int insertPath(int rootId, Path dir) throws SQLException {
        String relPath = root.relativize(dir).toString();

            relPathInsert.clearParameters();
            relPathInsert.setInt(1, rootId);
            relPathInsert.setString(2, relPath);
            relPathInsert.execute();
            ResultSet generatedKeys = relPathInsert.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            }
//                System.out.println("inserting: " + relPathInsert.execute());
        return -1;
    }


    private int upsertRoot(Path root) throws SQLException {
        Integer id = getRootId(root);
        if (id != null) {
            return id;
        }

        String sql = "INSERT INTO "+Tables.ROOTS_TABLE_NAME +"(ROOT_PATH) VALUES (?)";
        PreparedStatement insert = connection.prepareStatement(sql);
        insert.setString(1, root.toAbsolutePath().toString());

        insert.execute();
        id = getRootId(root);
        if (id == null) {
            throw new SQLException("FAILED TO SET ROOT");
        }
        return id;
    }

    private Integer getRootId(Path p) throws SQLException {
        String sql = "SELECT ROOT_ID FROM "+Tables.ROOTS_TABLE_NAME +
                " WHERE ROOT_PATH=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, p.toAbsolutePath().toString());
        ResultSet rs = ps.executeQuery();
        int i = 0;
        int result = -1;
        while (rs.next()) {
            result = rs.getInt(1);
            //check for wasNull??
            i++;
        }
        if (i == 1) {
            return result;
        }
        if (i > 1) {
            throw new SQLException("UNIQUE CONSTRAINT FAILURE with "+root.toString());
        }
        return null;
    }


}
