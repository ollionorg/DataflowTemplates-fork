package com.google.cloud.teleport.v2.templates.dbutils.connection;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.IShard;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraConnectionHelper implements IConnectionHelper<CqlSession> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectionHelper.class);
    private static Map<String, CqlSession> connectionPoolMap = null;

    @Override
    public synchronized void init(ConnectionHelperRequest connectionHelperRequest) {
        if (connectionPoolMap != null) {
            return;
        }
        LOG.info("Initializing Cassandra connection pool with size: {}", connectionHelperRequest.getMaxConnections());
        connectionPoolMap = new HashMap<>();
        List<IShard> iShards= connectionHelperRequest.getShards();

        for(IShard ishard: iShards) {
            CassandraShard cassandraShard = (CassandraShard) ishard;
            cassandraShard.validate();

            CqlSessionBuilder builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(cassandraShard.getHost(), Integer.parseInt(cassandraShard.getPort())))
                    .withAuthCredentials(cassandraShard.getUser(), cassandraShard.getPassword())
                    .withKeyspace(cassandraShard.getKeySpaceName());

            ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();
            configLoaderBuilder.withInt((DriverOption) TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, cassandraShard.getLocalPoolSize());
            configLoaderBuilder.withInt((DriverOption) TypedDriverOption.CONNECTION_POOL_REMOTE_SIZE, cassandraShard.getRemotePoolSize());
            builder.withConfigLoader(configLoaderBuilder.build());

            CqlSession session = builder.build();
            String connectionKey = cassandraShard.getHost() + ":" + cassandraShard.getPort() + "/" + cassandraShard.getUser() + "/" + cassandraShard.getKeySpaceName();
            connectionPoolMap.put(connectionKey, session);
        }


    }

    @Override
    public CqlSession getConnection(String connectionRequestKey) throws ConnectionException {
        try {
            if (connectionPoolMap == null) {
                LOG.warn("Connection pool not initialized");
                return null;
            }
            CqlSession session = connectionPoolMap.get(connectionRequestKey);
            if (session == null) {
                LOG.warn("Connection pool not found for source connection: {}", connectionRequestKey);
                return null;
            }
            return session;
        } catch (Exception e) {
            throw new ConnectionException(e);
        }
    }

    @Override
    public boolean isConnectionPoolInitialized() {
        return false;
    }

    // for unit testing
    public void setConnectionPoolMap(Map<String, CqlSession> inputMap) {
        connectionPoolMap = inputMap;
    }
}