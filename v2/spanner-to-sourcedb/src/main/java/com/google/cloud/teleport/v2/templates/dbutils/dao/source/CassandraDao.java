package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class CassandraDao implements IDao<DMLGeneratorResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraDao.class);
  private final String cassandraUrl;
  private final String cassandraUser;
  private final IConnectionHelper connectionHelper;

  public CassandraDao(String cassandraUrl, String cassandraUser, IConnectionHelper connectionHelper) {
    this.cassandraUrl = cassandraUrl;
    this.cassandraUser = cassandraUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(DMLGeneratorResponse dmlGeneratorResponse) throws Exception {
      try (CqlSession session = (CqlSession) connectionHelper.getConnection(this.cassandraUrl)) { // Ensure connection is obtained
          if (session == null) {
              throw new ConnectionException("Connection is null");
          }
          if (dmlGeneratorResponse instanceof PreparedStatementGeneratedResponse) {
              PreparedStatementGeneratedResponse preparedStatementGeneratedResponse = (PreparedStatementGeneratedResponse) dmlGeneratorResponse;
              try {
                  String dmlStatement = preparedStatementGeneratedResponse.getDmlStatement();
                  PreparedStatement preparedStatement = session.prepare(dmlStatement);

                  List<Object> values = preparedStatementGeneratedResponse.getValues();
                  if (dmlStatement.chars().filter(ch -> ch == '?').count() != values.size()) {
                      throw new IllegalArgumentException("Mismatch between placeholders and parameter count.");
                  }
                  BoundStatement boundStatement = preparedStatement.bind(values.toArray());
                  session.execute(boundStatement);
              }catch (Exception e){
                  LOG.error(e.getMessage());
              }

          } else {
              String simpleStatement = dmlGeneratorResponse.getDmlStatement();
              session.execute(simpleStatement);
          }
      }
  }
}