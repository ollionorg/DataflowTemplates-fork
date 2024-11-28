package com.google.cloud.teleport.v2.spanner.migrations.cassandra;

import java.io.Serializable;
import java.util.Objects;

public class CassandraConfig implements Serializable {

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

  public CassandraConfig() {}

  public CassandraConfig(
      String host,
      String port,
      String username,
      String password,
      String keyspace,
      String consistencyLevel,
      boolean sslOptions,
      String protocolVersion,
      String dataCenter,
      int localPoolSize,
      int remotePoolSize) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.keyspace = keyspace;
    this.consistencyLevel = consistencyLevel;
    this.sslOptions = sslOptions;
    this.protocolVersion = protocolVersion;
    this.dataCenter = dataCenter;
    this.localPoolSize = localPoolSize;
    this.remotePoolSize = remotePoolSize;
  }

  // Getter and Setter methods for all fields

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getConsistencyLevel() {
    return consistencyLevel;
  }

  public void setConsistencyLevel(String consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  public boolean isSslOptions() {
    return sslOptions;
  }

  public void setSslOptions(boolean sslOptions) {
    this.sslOptions = sslOptions;
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(String protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public int getLocalPoolSize() {
    return localPoolSize;
  }

  public void setLocalPoolSize(int localPoolSize) {
    this.localPoolSize = localPoolSize;
  }

  public int getRemotePoolSize() {
    return remotePoolSize;
  }

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
    return "CassandraConfig{"
        + "host='" + host + '\''
        + ", port='" + port + '\''
        + ", username='" + username + '\''
        + ", password='" + password + '\''
        + ", keyspace='" + keyspace + '\''
        + ", consistencyLevel='" + consistencyLevel + '\''
        + ", sslOptions=" + sslOptions
        + ", protocolVersion='" + protocolVersion + '\''
        + ", dataCenter='" + dataCenter + '\''
        + ", localPoolSize=" + localPoolSize
        + ", remotePoolSize=" + remotePoolSize
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CassandraConfig)) {
      return false;
    }
    CassandraConfig cassandraConfig = (CassandraConfig) o;
    return sslOptions == cassandraConfig.sslOptions
        && localPoolSize == cassandraConfig.localPoolSize
        && remotePoolSize == cassandraConfig.remotePoolSize
        && Objects.equals(host, cassandraConfig.host)
        && Objects.equals(port, cassandraConfig.port)
        && Objects.equals(username, cassandraConfig.username)
        && Objects.equals(password, cassandraConfig.password)
        && Objects.equals(keyspace, cassandraConfig.keyspace)
        && Objects.equals(consistencyLevel, cassandraConfig.consistencyLevel)
        && Objects.equals(protocolVersion, cassandraConfig.protocolVersion)
        && Objects.equals(dataCenter, cassandraConfig.dataCenter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        host,
        port,
        username,
        password,
        keyspace,
        consistencyLevel,
        sslOptions,
        protocolVersion,
        dataCenter,
        localPoolSize,
        remotePoolSize);
  }
}