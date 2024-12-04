/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class JdbcDao implements IDao<DMLGeneratorResponse,ResultSet> {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDao.class);
  private String sqlUrl;
  private String sqlUser;

  private final IConnectionHelper connectionHelper;

  public JdbcDao(String sqlUrl, String sqlUser, IConnectionHelper connectionHelper) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public ResultSet execute(DMLGeneratorResponse dmlGeneratorResponse) throws SQLException, ConnectionException {
    Connection connObj = null;
    try {
      connObj = (Connection) connectionHelper.getConnection(this.sqlUrl + "/" + this.sqlUser);
      if (connObj == null) {
        throw new ConnectionException("Connection is null");
      }
      if (dmlGeneratorResponse instanceof PreparedStatementGeneratedResponse) {
        PreparedStatementGeneratedResponse preparedStatementGeneratedResponse = (PreparedStatementGeneratedResponse) dmlGeneratorResponse;
        try (PreparedStatement statement = connObj.prepareStatement(preparedStatementGeneratedResponse.getDmlStatement())) {
          int index = 1;
          for (Object value : preparedStatementGeneratedResponse.getValues()) {
            statement.setObject(index++, value); // Bind values to placeholders
          }
          statement.executeUpdate();
          return  null;
        }
      } else {
        try (Statement statement = connObj.createStatement()) {
          statement.executeUpdate(dmlGeneratorResponse.getDmlStatement());
          return  null;
        }
      }
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    } finally {
      if (connObj != null) {
        try {
          connObj.close();
        } catch (SQLException e) {
          LOG.error(e.getMessage());
        }
      }
    }
    return null;
  }

  @Override
  public String read(String statement) throws Exception {
    Connection connObj = null;
    Statement stmt = null;

    try {
      connObj = (Connection) connectionHelper.getConnection(this.sqlUrl + "/" + this.sqlUser);
      if (connObj == null) {
        throw new ConnectionException("Connection is null");
      }
      stmt = connObj.createStatement();
      return stmt.executeQuery(statement).toString();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (connObj != null) {
        connObj.close();
      }
    }
  }
}
