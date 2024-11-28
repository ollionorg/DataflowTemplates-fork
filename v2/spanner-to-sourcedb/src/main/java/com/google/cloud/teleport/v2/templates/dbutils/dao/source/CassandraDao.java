package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.cloud.teleport.v2.templates.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.utils.connection.IConnectionHelper;

public class CassandraDao implements IDao<String> {
  private final String cassandraUrl;
  private final String cassandraUser;
  private final IConnectionHelper<CqlSession> connectionHelper;

  public CassandraDao(String cassandraUrl, String cassandraUser, IConnectionHelper<CqlSession> connectionHelper) {
    this.cassandraUrl = cassandraUrl;
    this.cassandraUser = cassandraUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(String cqlStatement) throws Exception {
    CqlSession session = null;

    try {
      session = connectionHelper.getConnection(this.cassandraUrl + "/" + this.cassandraUser);
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