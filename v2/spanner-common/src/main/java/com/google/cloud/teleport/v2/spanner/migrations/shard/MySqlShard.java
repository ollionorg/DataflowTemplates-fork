/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.shard;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MySqlShard implements IShard {

    private String logicalShardId;
    private String host;
    private String port;
    private String user;
    private String password;
    private String dbName;
    private String namespace;
    private String secretManagerUri;
    private String connectionProperties;

    private Map<String, String> dbNameToLogicalShardIdMap = new HashMap<>();

    public MySqlShard(
            String logicalShardId,
            String host,
            String port,
            String user,
            String password,
            String dbName,
            String namespace,
            String secretManagerUri,
            String connectionProperties) {
        this.logicalShardId = logicalShardId;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.namespace = namespace;
        this.secretManagerUri = secretManagerUri;
        this.connectionProperties = connectionProperties;
    }

    public MySqlShard() {
    }

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
        return user;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public String getSecretManagerUri() {
        return secretManagerUri;
    }

    @Override
    public String getConnectionProperties() {
        return connectionProperties;
    }

    @Override
    public Map<String, String> getDbNameToLogicalShardIdMap() {
        return dbNameToLogicalShardIdMap;
    }

    @Override
    public String getKeySpaceName() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public String getConsistencyLevel() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public Boolean getSSLOptions() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public String getProtocolVersion() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public String getDataCenter() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public Integer getLocalPoolSize() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public Integer getRemotePoolSize() {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
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
        this.user = user;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public void setSecretManagerUri(String secretManagerUri) {
        this.secretManagerUri = secretManagerUri;
    }

    @Override
    public void setConnectionProperties(String connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    @Override
    public void setDbNameToLogicalShardIdMap(Map<String, String> dbNameToLogicalShardIdMap) {
        this.dbNameToLogicalShardIdMap = dbNameToLogicalShardIdMap;
    }

    @Override
    public void setKeySpaceName(String keySpaceName) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public void setConsistencyLevel(String consistencyLevel) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public void setSslOptions(boolean sslOptions) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public void setProtocolVersion(String protocolVersion) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public void setDataCenter(String dataCenter) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public void setLocalPoolSize(int localPoolSize) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public void setRemotePoolSize(int remotePoolSize) {
        throw new IllegalArgumentException("Not Supported in Mysql Shard");
    }

    @Override
    public String toString() {
        return "MySqlShard{"
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
                + user
                + '\''
                + ", dbName='"
                + dbName
                + '\''
                + ", namespace='"
                + namespace
                + '\''
                + ", connectionProperties='"
                + connectionProperties
                + '\''
                + ", dbNameToLogicalShardIdMap="
                + dbNameToLogicalShardIdMap
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlShard)) {
            return false;
        }
        MySqlShard mySqlShard = (MySqlShard) o;
        return Objects.equals(logicalShardId, mySqlShard.logicalShardId)
                && Objects.equals(host, mySqlShard.host)
                && Objects.equals(port, mySqlShard.port)
                && Objects.equals(user, mySqlShard.user)
                && Objects.equals(password, mySqlShard.password)
                && Objects.equals(dbName, mySqlShard.dbName)
                && Objects.equals(namespace, mySqlShard.namespace)
                && Objects.equals(connectionProperties, mySqlShard.connectionProperties)
                && Objects.equals(secretManagerUri, mySqlShard.secretManagerUri)
                && Objects.equals(dbNameToLogicalShardIdMap, mySqlShard.dbNameToLogicalShardIdMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                logicalShardId,
                host,
                port,
                user,
                password,
                dbName,
                namespace,
                connectionProperties,
                secretManagerUri,
                dbNameToLogicalShardIdMap);
    }
}
