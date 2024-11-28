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
<<<<<<<< HEAD:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dao/JdbcDaoTest.java
package com.google.cloud.teleport.v2.templates.dao;
========
package com.google.cloud.teleport.v2.templates.dbutils.dao;
>>>>>>>> dev-repackaged:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dbutils/dao/JdbcDaoTest.java

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

<<<<<<<< HEAD:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dao/JdbcDaoTest.java
import com.google.cloud.teleport.v2.templates.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.utils.connection.MySQLConnectionHelper;
========
import com.google.cloud.teleport.v2.templates.dbutils.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
>>>>>>>> dev-repackaged:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dbutils/dao/JdbcDaoTest.java
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public final class JdbcDaoTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private HikariDataSource mockHikariDataSource;
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;

  @Before
  public void doBeforeEachTest() throws java.sql.SQLException {
    when(mockHikariDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeUpdate(any())).thenReturn(1);
    doNothing().when(mockStatement).close();
    doNothing().when(mockConnection).close();
  }

  @Test(expected = ConnectionException.class)
  public void testNullConnection() throws java.sql.SQLException, ConnectionException {
<<<<<<<< HEAD:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dao/JdbcDaoTest.java
    JdbcDao sqlDao = new JdbcDao("url", "user", new MySQLConnectionHelper());
========
    JdbcDao sqlDao = new JdbcDao("url", "user", new JdbcConnectionHelper());
>>>>>>>> dev-repackaged:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dbutils/dao/JdbcDaoTest.java
    sqlDao.write("sql");
  }

  @Test
  public void testSuccess() throws java.sql.SQLException, ConnectionException {
    Map<String, HikariDataSource> connectionPoolMap = new HashMap<>();
    connectionPoolMap.put("url/user", mockHikariDataSource);
<<<<<<<< HEAD:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dao/JdbcDaoTest.java
    MySQLConnectionHelper mySQLConnectionHelper = new MySQLConnectionHelper();
    mySQLConnectionHelper.setConnectionPoolMap(connectionPoolMap);
    JdbcDao sqlDao = new JdbcDao("url", "user", mySQLConnectionHelper);
========
    JdbcConnectionHelper jdbcConnectionHelper = new JdbcConnectionHelper();
    jdbcConnectionHelper.setConnectionPoolMap(connectionPoolMap);
    JdbcDao sqlDao = new JdbcDao("url", "user", jdbcConnectionHelper);
>>>>>>>> dev-repackaged:v2/spanner-to-sourcedb/src/test/java/com/google/cloud/teleport/v2/templates/dbutils/dao/JdbcDaoTest.java
    sqlDao.write("sql");
    verify(mockStatement).executeUpdate(eq("sql"));
  }
}
