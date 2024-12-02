package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;

public class CassandraDao implements IDao<String> {
  private final String cassandraUrl;
  private final String cassandraUser;
  private final IConnectionHelper connectionHelper;

  public CassandraDao(String cassandraUrl, String cassandraUser, IConnectionHelper connectionHelper) {
    this.cassandraUrl = cassandraUrl;
    this.cassandraUser = cassandraUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(String cqlStatement) throws Exception {
    CqlSession session = null;

    try {
      session = (CqlSession) connectionHelper.getConnection(this.cassandraUrl);
      if (session == null) {
        throw new ConnectionException("Connection is null");
      }
      SimpleStatement statement = SimpleStatement.newInstance(cqlStatement);
      session.execute(statement);
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }
}