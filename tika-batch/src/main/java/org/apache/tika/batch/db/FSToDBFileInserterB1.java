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
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.tika.batch.FileResourceCrawler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to crawl file structure and insert files
 * into the db with status of
 * {@link org.apache.tika.batch.db.BatchDBConstants.FILE_STATUS#NEEDS_TO_BE_PROCESSED}
 *
 * <p/>
 * This assumes that no other thread is modifying the table!!!
 */
public class FSToDBFileInserterB1 implements Callable<FileInserterStatus> {
    final static Logger logger = LoggerFactory.getLogger(FileResourceCrawler.class.toString());

    final Path root;
    final Connection connection;
    final boolean append;

    PreparedStatement relPathExists;
    PreparedStatement relPathInsert;
    PreparedStatement fileStatusExists;
    PreparedStatement insertFileStatusStatement;

    public FSToDBFileInserterB1(Path root, Connection connection, boolean append) {
        this.root = root;
        this.connection = connection;
        this.append = append;
    }

    @Override
    public FileInserterStatus call() throws Exception {
        int rootId = upsertRoot(root);
        String sql = "SELECT PATH_ID FROM "+ Tables.REL_PATHS_TABLE_NAME +
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
        /*
        sql = "MERGE INTO "+ Tables.FILE_STATUS_TABLE_NAME +
                " (PATH_ID, FILE_NAME, PROCESSING_STATUS)" +
                " KEY (PATH_ID, FILE_NAME) VALUES (?,?,?)";
*/
        sql = "INSERT INTO "+ Tables.FILE_STATUS_TABLE_NAME +
                " (PATH_ID, FILE_NAME, PROCESSING_STATUS)"+
                " VALUES (?,?,?)";
        insertFileStatusStatement =
                connection.prepareStatement(sql);

        FileInserter fileInserter = new FileInserter(rootId);
        Files.walkFileTree(root, fileInserter);

        return new FileInserterStatus();
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

    private class FileInserter implements FileVisitor<Path> {
        final int rootId;
        int added = 0;
        FileInserter(int rootId) {
            this.rootId = rootId;
        }
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            String relPath = root.relativize(dir).toString();
            //System.out.println("REL_PATH: " + relPath);
            int exists = 0;
            try {
                relPathExists.clearParameters();
                relPathExists.setString(1, relPath);
                ResultSet rs = relPathExists.executeQuery();
                while (rs.next()) {
                    exists++;
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            if (exists > 0) {
//                System.out.println("rel path exists");
                return FileVisitResult.CONTINUE;
            }

            //TODO: check for >1
            try {
                relPathInsert.clearParameters();
                relPathInsert.setInt(1, rootId);
                relPathInsert.setString(2, relPath);
                relPathInsert.execute();
//                System.out.println("inserting: " + relPathInsert.execute());
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            try {
                int pathId = getPathId(file);
                if (pathId < 0) {
                    throw new RuntimeException("couldn't find path id");
                }

                if (!Files.isReadable(file)) {
                    logger.warn("Skipping -- " + file.toAbsolutePath() +
                            " -- file/directory is not readable");
                    return FileVisitResult.CONTINUE;
                }

                String fileName = file.getFileName().toString();
                insertFileStatusStatement.clearParameters();
                insertFileStatusStatement.setInt(1, pathId);
                insertFileStatusStatement.setString(2, fileName);
                insertFileStatusStatement.setInt(3, BatchDBConstants.FILE_STATUS.NEEDS_TO_BE_PROCESSED.ordinal());
//  postgres              insertFileStatusStatement.setInt(4, pathId);
  //              insertFileStatusStatement.setString(5, fileName);
                boolean inserted = insertFileStatusStatement.execute();
//                System.out.println("inserting: " + fileName + " : " +inserted);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //if (added++ > 50000) {
              //  return FileVisitResult.TERMINATE;
            //}
            return FileVisitResult.CONTINUE;
        }

        private int getPathId(Path file) {
            String relPath = root.relativize(file.getParent()).toString();
           // System.out.println("RELPATH FOUND: >"+relPath+"<");
            String sql = "SELECT * FROM " + Tables.REL_PATHS.getTableName();
            /*try {
                ResultSet rs = connection.prepareStatement(sql).executeQuery();
                ResultSetMetaData m = rs.getMetaData();
                System.out.println("ABOUT TO DUMP RELPATHS");
                int rows = 0;
                while (rs.next()) {
                    System.out.print(rows++ + "::\t");
                    for (int i = 1; i <= m.getColumnCount(); i++) {
                        System.out.print(rs.getString(i)+"\t");
                    }
                    System.out.println("");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }*/

            try {
                relPathExists.clearParameters();
                relPathExists.setString(1, relPath);
                ResultSet rs = relPathExists.executeQuery();
                int ret = -1;
                int cnt = 0;
                while (rs.next()) {
                    ret = rs.getInt(1);
                    cnt++;
                }
                if (cnt > 1) {
                    throw new RuntimeException("Non unique path id: "+relPath);
                }
                return ret;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return -1;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }
}
