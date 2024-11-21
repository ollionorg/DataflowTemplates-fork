package com.google.cloud.teleport.v2.templates.utils;

/*
 * Copyright (C) 2024 Your Company
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CassandraDaoTest {

    @Rule public final MockitoRule mockito = MockitoJUnit.rule();

    @Mock private CassandraConnection mockCassandraConnection;

    @Mock private CqlSession mockCqlSession;

    private CassandraDao cassandraDao;
    String host = "127.0.0.1";
    int port = 9042;
    String datacenter = "datacenter1";
    String username = "";
    String password = "";

    @Before
    public void setUp() throws Exception {
        // Mock the behavior of CassandraConnection and CqlSession
        when(mockCassandraConnection.getSession()).thenReturn(mockCqlSession);
        doNothing().when(mockCassandraConnection).close();

        // Initialize CassandraDao with mock parameters
        cassandraDao = new CassandraDao(host, port, datacenter, username, "password");
    }

    @Test(expected = RuntimeException.class)
    public void testNullConnection() throws Exception {
        // Simulate getInstance() throwing an exception
        when(CassandraConnection.getInstance(
                host, port, datacenter, username, password))
                .thenThrow(new RuntimeException("Connection failed"));

        // Attempt to write, expecting an exception
        cassandraDao.write("INSERT INTO test_table (id, value) VALUES (1, 'test')");
    }

    @Test
    public void testWriteSuccess() throws Exception {
        try (MockedStatic<CassandraConnection> mockedStatic = Mockito.mockStatic(CassandraConnection.class)) {
            mockedStatic
                    .when(() -> CassandraConnection.getInstance(
                            host, port, datacenter, username, password))
                    .thenReturn(mockCassandraConnection);
            cassandraDao.write("INSERT INTO test_table (id, value) VALUES (1, 'test')");
            verify(mockCqlSession, times(1)).execute("INSERT INTO test_table (id, value) VALUES (1, 'test')");
            verify(mockCassandraConnection, times(1)).close();
        }
    }
}
