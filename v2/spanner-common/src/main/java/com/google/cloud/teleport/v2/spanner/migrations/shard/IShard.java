package com.google.cloud.teleport.v2.spanner.migrations.shard;

import java.io.Serializable;
import java.util.Map;

public interface IShard extends Serializable {
    // Getter methods
    String getLogicalShardId();
    String getHost();
    String getPort();
    String getUser();
    String getPassword();
    String getDbName();
    String getNamespace();
    String getSecretManagerUri();
    String getConnectionProperties();
    Map<String, String> getDbNameToLogicalShardIdMap();

    // Cassandra Configuration
    String getKeySpaceName();
    String getConsistencyLevel();
    Boolean getSSLOptions();
    String getProtocolVersion();
    String getDataCenter();
    Integer getLocalPoolSize();
    Integer getRemotePoolSize();


    // Setter methods
    void setLogicalShardId(String logicalShardId);
    void setHost(String host);
    void setPort(String port);
    void setUser(String user);
    void setPassword(String password);
    void setDbName(String dbName);
    void setNamespace(String namespace);
    void setSecretManagerUri(String secretManagerUri);
    void setConnectionProperties(String connectionProperties);
    void setDbNameToLogicalShardIdMap(Map<String, String> dbNameToLogicalShardIdMap);

    // cassandra configuration
    void setKeySpaceName(String keySpaceName);
    void setConsistencyLevel(String consistencyLevel);
    void setSslOptions(boolean sslOptions);
    void setProtocolVersion(String protocolVersion);
    void setDataCenter(String dataCenter);
    void setLocalPoolSize(int localPoolSize);
    void setRemotePoolSize(int remotePoolSize);

}
