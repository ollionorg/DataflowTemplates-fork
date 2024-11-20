package com.google.cloud.teleport.v2.templates.utils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;

import java.net.InetSocketAddress;

public class CassandraConnection {
    private static CassandraConnection instance;
    private CqlSession session;

    // Private constructor to enforce Singleton
    private CassandraConnection(String host, int port, String datacenter, String username, String password) {
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host, port))
                    .withLocalDatacenter(datacenter)
                   // .withAuthProvider(new ProgrammaticPlainTextAuthProvider(username, password)) // Use if authentication is enabled
                    .build();
            session.execute("USE mykeyspace;");

            System.out.println("Connected to Cassandra!");
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Cassandra", e);
        }
    }
    public static synchronized CassandraConnection getInstance(String host, int port, String datacenter, String username, String password) {
        if (instance == null) {
            instance = new CassandraConnection(host, port, datacenter, username, password);
        }
        return instance;
    }

    // Method to get the CqlSession
    public CqlSession getSession() {
        return session;
    }

    // Close the session when done
    public void close() {
        if (session != null) {
            session.close();
            System.out.println("Cassandra connection closed.");
        }
    }
}