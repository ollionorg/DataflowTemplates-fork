package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.cassandra.CassandraConfig;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read the Cassandra configuration file in GCS and convert it into a CassandraConfig object. */
public class CassandraConfigFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraConfigFileReader.class);

    public CassandraConfig getCassandraConfig(String cassandraConfigFilePath) {
        try (InputStream stream =
                     Channels.newInputStream(
                             FileSystems.open(FileSystems.matchNewResource(cassandraConfigFilePath, false)))) {

            String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
            CassandraConfig cassandraConfig =
                    new GsonBuilder()
                            .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
                            .create()
                            .fromJson(result, CassandraConfig.class);

            LOG.info("The Cassandra config is: {}", cassandraConfig);
            return cassandraConfig;

        } catch (IOException e) {
            LOG.error(
                    "Failed to read Cassandra config file. Make sure it is ASCII or UTF-8 encoded and contains a well-formed JSON string.",
                    e);
            throw new RuntimeException(
                    "Failed to read Cassandra config file. Make sure it is ASCII or UTF-8 encoded and contains a well-formed JSON string.",
                    e);
        }
    }
}