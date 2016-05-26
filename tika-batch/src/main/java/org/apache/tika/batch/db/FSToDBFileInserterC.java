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
public class FSToDBFileInserterC implements Callable<FileInserterStatus> {
    final static Logger logger = LoggerFactory.getLogger(FileResourceCrawler.class.toString());

    final Path root;
    final Connection connection;
    final boolean append;

    PreparedStatement insertFileStatusStatement;

    public FSToDBFileInserterC(Path root, Connection connection, boolean append) {
        this.root = root;
        this.connection = connection;
        this.append = append;
    }

    @Override
    public FileInserterStatus call() throws Exception {

        String sql = "INSERT INTO "+ Tables.FLAT_TABLE_NAME +
                " (FILE_PATH, PROCESSING_STATUS)"+
                " VALUES (?,?)";
        insertFileStatusStatement =
                connection.prepareStatement(sql);

        addFiles(root);

        return new FileInserterStatus();
    }

    private void addFiles(Path directory) throws InterruptedException {

        if (directory == null) {
            logger.warn("FSToDBFileInserter asked to process null directory?!");
            return;
        }

        List<Path> directories = new LinkedList<>();
        List<Path> files = new LinkedList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(directory)){
            for (Path p : ds) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("file adder interrupted");
                }
                if (Files.isDirectory(p)) {
                    directories.add(p);
                } else {
                    files.add(p);
                }
            }
        } catch (IOException e) {
            logger.warn("FSFileAdder couldn't read "+directory.toAbsolutePath() +
                    ": "+e.getMessage());
        }
        for (Path f : files) {
            tryToInsertFile(f);
        }
        for (Path dir : directories) {
            addFiles(dir);
        }
    }

    private void tryToInsertFile(Path p) {

        if (!Files.isReadable(p)) {
            logger.warn("Skipping -- " + p.toAbsolutePath() +
                    " -- file/directory is not readable");
            return;
        }

        String relPath = root.relativize(p).toString();
        try {
            insertFileStatusStatement.clearParameters();
            insertFileStatusStatement.setString(1, relPath);
            insertFileStatusStatement.setInt(2, BatchDBConstants.FILE_STATUS.NEEDS_TO_BE_PROCESSED.ordinal());
            boolean ex = insertFileStatusStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

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
