package com.ssau.processor.service;

import java.util.Properties;
import java.util.UUID;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraService {
    
    private static final String KEYSPACE = "dvaf";
    private static final String TABLE = "processing_results";
    
    private CqlSession session;
    private PreparedStatement insertStatement;
    
    public CassandraService(Properties props) {
        String contactPoint = props.getProperty("cassandra.contact.point", "127.0.0.1");
        int port = Integer.parseInt(props.getProperty("cassandra.port", "9042"));
        String keyspace = props.getProperty("cassandra.keyspace", KEYSPACE);
        int connectionTimeoutMs = Integer.parseInt(props.getProperty("cassandra.connection.timeout.ms", "10000"));
        int requestTimeoutMs = Integer.parseInt(props.getProperty("cassandra.request.timeout.ms", "10000"));
        
        log.info("Connecting to Cassandra at {}:{} (connection timeout: {}ms, request timeout: {}ms)", 
            contactPoint, port, connectionTimeoutMs, requestTimeoutMs);
        
        int maxRetries = 5;
        int retryDelayMs = 2000;
        Exception lastException = null;
        CqlSession tempSession = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(connectionTimeoutMs))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(requestTimeoutMs))
                    .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofMillis(connectionTimeoutMs))
                    .build();
                
                CqlSessionBuilder builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(contactPoint, port))
                    .withLocalDatacenter("datacenter1")
                    .withConfigLoader(configLoader);
                
                tempSession = builder.build();
                
                tempSession.execute("SELECT release_version FROM system.local");
                log.info("Successfully connected to Cassandra (attempt {}/{})", attempt, maxRetries);
                
                this.session = tempSession;
                tempSession = null;
                
                createKeyspace(keyspace);
                createTable(keyspace);
                
                String insertCql = String.format(
                    "INSERT INTO %s.%s (" +
                    "camera_id, day, frame_timestamp, id, processing_timestamp, " +
                    "detection_type, detection_count, frame_rows, frame_cols, image_path, metadata" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    keyspace, TABLE
                );
                this.insertStatement = session.prepare(insertCql);
                
                log.info("Cassandra service initialized successfully");
                return;
                
            } catch (Exception e) {
                lastException = e;
                log.warn("Failed to connect to Cassandra (attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
            
                if (tempSession != null) {
                    try {
                        tempSession.close();
                    } catch (Exception closeEx) {
                        log.warn("Error closing failed session", closeEx);
                    }
                    tempSession = null;
                }
                
                if (attempt < maxRetries) {
                    try {
                        log.info("Retrying in {} ms...", retryDelayMs);
                        Thread.sleep(retryDelayMs);
                        retryDelayMs *= 2;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying Cassandra connection", ie);
                    }
                }
            }
        }
        
        log.error("Failed to connect to Cassandra after {} attempts", maxRetries);
        throw new RuntimeException("Failed to connect to Cassandra after " + maxRetries + " attempts", lastException);
    }
    
    private void createKeyspace(String keyspace) {
        String createKeyspaceCql = String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s " +
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            keyspace
        );
        
        try {
            session.execute(SimpleStatement.newInstance(createKeyspaceCql));
            log.info("Keyspace '{}' created or already exists", keyspace);
        } catch (Exception e) {
            log.error("Failed to create keyspace '{}'", keyspace, e);
            throw new RuntimeException("Failed to create keyspace", e);
        }
    }
    
    private void createTable(String keyspace) {
        String createTableCql = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "camera_id TEXT, " +
            "day TEXT, " +
            "frame_timestamp TIMESTAMP, " +
            "id UUID, " +
            "processing_timestamp TIMESTAMP, " +
            "detection_type TEXT, " +
            "detection_count INT, " +
            "frame_rows INT, " +
            "frame_cols INT, " +
            "image_path TEXT, " +
            "metadata TEXT, " +
            "PRIMARY KEY ((camera_id, day), frame_timestamp, id)" +
            ") WITH CLUSTERING ORDER BY (frame_timestamp DESC)",
            keyspace, TABLE
        );

        session.execute(SimpleStatement.newInstance(createTableCql));
    }
    
    public void insertResult(String cameraId, Instant frameTimestamp, String detectionType,
                            int detectionCount, int frameRows, int frameCols,
                            String imagePath, String metadata) {
        try {
            UUID id = UUID.randomUUID();
            Instant processingTimestamp = Instant.now();
            String day = frameTimestamp.atZone(ZoneOffset.UTC).toLocalDate().toString();

            session.execute(insertStatement.bind(
                cameraId, day, frameTimestamp, id, processingTimestamp,
                detectionType, detectionCount, frameRows, frameCols,
                imagePath != null ? imagePath : "",
                metadata != null ? metadata : ""
            ));

            log.debug("Inserted processing result for camera {} at {}", cameraId, frameTimestamp);
        } catch (Exception e) {
            log.error("Failed to insert processing result for camera {}", cameraId, e);
        }
    }
    
    public void close() {
        if (session != null) {
            session.close();
            log.info("Cassandra session closed");
        }
    }
}
