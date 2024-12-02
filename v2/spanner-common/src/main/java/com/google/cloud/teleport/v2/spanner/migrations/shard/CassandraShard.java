package com.google.cloud.teleport.v2.spanner.migrations.shard;

import java.util.Map;
import java.util.Objects;

public class CassandraShard implements IShard {
    private String logicalShardId;
    private String host;
    private String port;
    private String username;
    private String password;
    private String keyspace;
    private String consistencyLevel = "LOCAL_QUORUM";
    private boolean sslOptions = false;
    private String protocolVersion = "v5";
    private String dataCenter = "datacenter1";
    private int localPoolSize = 1024;
    private int remotePoolSize = 256;

    @Override
    public String getLogicalShardId() {
        return logicalShardId;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getPort() {
        return port;
    }

    @Override
    public String getUser() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDbName() {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public String getNamespace() {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public String getSecretManagerUri() {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public String getConnectionProperties() {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public Map<String, String> getDbNameToLogicalShardIdMap() {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public String getKeySpaceName() {
        return keyspace;
    }

    @Override
    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    @Override
    public Boolean getSSLOptions() {
        return sslOptions;
    }

    @Override
    public String getProtocolVersion() {
        return protocolVersion;
    }

    @Override
    public String getDataCenter() {
        return dataCenter;
    }

    @Override
    public Integer getLocalPoolSize() {
        return localPoolSize;
    }

    @Override
    public Integer getRemotePoolSize() {
        return remotePoolSize;
    }

    @Override
    public void setLogicalShardId(String logicalShardId) {
        this.logicalShardId = logicalShardId;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public void setUser(String user) {
        this.username = user;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public void setDbName(String dbName) {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public void setNamespace(String namespace) {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public void setSecretManagerUri(String secretManagerUri) {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public void setConnectionProperties(String connectionProperties) {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public void setDbNameToLogicalShardIdMap(Map<String, String> dbNameToLogicalShardIdMap) {
        throw new IllegalArgumentException("Not Supported in Cassandra Shard");
    }

    @Override
    public void setKeySpaceName(String keySpaceName) {

    }

    @Override
    public void setConsistencyLevel(String consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public void setSslOptions(boolean sslOptions) {
        this.sslOptions = sslOptions;
    }

    @Override
    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    @Override
    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    @Override
    public void setLocalPoolSize(int localPoolSize) {
        this.localPoolSize = localPoolSize;
    }

    @Override
    public void setRemotePoolSize(int remotePoolSize) {
        this.remotePoolSize = remotePoolSize;
    }

    public void validate() throws IllegalArgumentException {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host is required");
        }
        if (port == null || port.isEmpty()) {
            throw new IllegalArgumentException("Port is required");
        }
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username is required");
        }
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Password is required");
        }
        if (keyspace == null || keyspace.isEmpty()) {
            throw new IllegalArgumentException("Keyspace is required");
        }
    }

    @Override
    public String toString() {
        return "CassandraShard{"
                + "logicalShardId='"
                + logicalShardId
                + '\''
                + ", host='"
                + host
                + '\''
                + ", port='"
                + port
                + '\''
                + ", user='"
                + username
                + '\''
                + ", keySpaceName='"
                + keyspace
                + '\''
                + ", datacenter='"
                + dataCenter
                + '\''
                + ", consistencyLevel='"
                + consistencyLevel
                + '\''
                + ", protocolVersion="
                + protocolVersion
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CassandraShard)) {
            return false;
        }
        CassandraShard cassandraShard = (CassandraShard) o;
        return Objects.equals(logicalShardId, cassandraShard.logicalShardId)
                && Objects.equals(host, cassandraShard.host)
                && Objects.equals(port, cassandraShard.port)
                && Objects.equals(username, cassandraShard.username)
                && Objects.equals(password, cassandraShard.password)
                && Objects.equals(keyspace, cassandraShard.keyspace)
                && Objects.equals(dataCenter, cassandraShard.dataCenter)
                && Objects.equals(consistencyLevel, cassandraShard.consistencyLevel)
                && Objects.equals(protocolVersion, cassandraShard.protocolVersion)
                && Objects.equals(sslOptions, cassandraShard.sslOptions)
                && Objects.equals(localPoolSize, cassandraShard.localPoolSize)
                && Objects.equals(remotePoolSize, cassandraShard.remotePoolSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                logicalShardId,
                host,
                port,
                username,
                password,
                keyspace,
                dataCenter,
                consistencyLevel,
                protocolVersion,
                sslOptions,
                localPoolSize,
                remotePoolSize);
    }
}
