package com.google.cloud.teleport.v2.templates.utils;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.core.CqlSession;
/** Writes data to MySQL. */
public class CassandraDao implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraDao.class);
    String cassandraHost;
    String cassandraUser;
    String cassandraPasswd;
    int port;
    String datacenter;

    public CassandraDao(String cassandraHost, int cassandraPort, String dataCenter, String cassandraUser, String cassandraPasswd) {
        this.cassandraHost = cassandraHost;
        this.port = cassandraPort;
        this.cassandraUser = cassandraUser;
        this.cassandraPasswd = cassandraPasswd;
        this.datacenter = dataCenter;
    }

    // writes to database
    public void write(String cassandraStatement) {
        CassandraConnection cassandraConnection = null;
        CqlSession session = null;

        try {
            cassandraConnection = CassandraConnection.getInstance(this.cassandraHost , this.port, this.datacenter, this.cassandraUser,this.cassandraPasswd);
            session = cassandraConnection.getSession();
            session.execute(cassandraStatement);
        } catch (Exception e) {
            System.err.println("An error occurred while interacting with Cassandra:");
            e.printStackTrace();
        } finally {
            // Ensure the connection is closed in the finally block
            if (cassandraConnection != null) {
                cassandraConnection.close();
            }
        }
    }
}

