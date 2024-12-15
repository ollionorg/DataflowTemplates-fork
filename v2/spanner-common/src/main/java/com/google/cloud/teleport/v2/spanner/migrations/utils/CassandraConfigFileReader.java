package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/** Class to read the Cassandra configuration file in GCS and convert it into a CassandraConfig object. */
public class CassandraConfigFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraConfigFileReader.class);

    public List<Shard> getCassandraShard(String cassandraConfigFilePath) {
        try (InputStream stream =
                     Channels.newInputStream(
                             FileSystems.open(FileSystems.matchNewResource(cassandraConfigFilePath, false)))) {

            String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
            Shard iShard =
                    new GsonBuilder()
                            .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
                            .create()
                            .fromJson(result, CassandraShard.class);

            LOG.info("The Cassandra config is: {}", iShard);
            return Collections.singletonList(iShard);

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