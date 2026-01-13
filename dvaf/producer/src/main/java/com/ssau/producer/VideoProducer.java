package com.ssau.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import lombok.extern.slf4j.Slf4j;

import com.ssau.producer.service.VideoEventCreator;
import com.ssau.producer.utils.ConfigLoader;
import com.ssau.producer.zookeeper.TopicAssignmentManager;

@Slf4j
public class VideoProducer {

    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        TopicAssignmentManager assignmentManager = null;
        try {
            Properties appProps = ConfigLoader.loadDefault();

            TopicInfo topicInfo = TopicInfo.fromProperties(appProps);
            assignmentManager = new TopicAssignmentManager(
                appProps.getProperty("zookeeper.connect"),
                appProps.getProperty("zookeeper.root.path", "/dvaf"),
                topicInfo.defaultTopic());
            assignmentManager.start();

            Producer<String, String> producer = createKafkaProducer(appProps);

            TopicAssignmentManager finalAssignmentManager = assignmentManager;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Kafka producer and thread pool...");
                producer.close();
                try {
                    finalAssignmentManager.close();
                } catch (Exception e) {
                    log.warn("Failed to close TopicAssignmentManager cleanly", e);
                }
                executor.shutdown();
            }));

            startCameraThreads(appProps, producer, assignmentManager, topicInfo.loadReportIntervalMs());
        } catch (Exception e) {
            log.error("Application error: {}", e.getMessage(), e);
        } finally {
            if (assignmentManager != null) {
                try {
                    assignmentManager.close();
                } catch (Exception e) {
                    log.debug("Failed to close TopicAssignmentManager on shutdown", e);
                }
            }
        }
    }

    private static Producer<String, String> createKafkaProducer(Properties appProps) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", appProps.getProperty("kafka.bootstrap.servers", "localhost:9092"));
        kafkaProps.put("acks", appProps.getProperty("kafka.acks", "all"));
        kafkaProps.put("retries", appProps.getProperty("kafka.retries", "3"));
        kafkaProps.put("batch.size", appProps.getProperty("kafka.batch.size", "16384"));
        kafkaProps.put("linger.ms", appProps.getProperty("kafka.linger.ms", "5"));
        kafkaProps.put("max.request.size", appProps.getProperty("kafka.max.request.size", "5242880"));
        kafkaProps.put("compression.type", appProps.getProperty("kafka.compression.type", "gzip"));
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(kafkaProps);
    }

    private static void startCameraThreads(Properties appProps,
                                           Producer<String, String> producer,
                                           TopicAssignmentManager assignmentManager,
                                           long loadReportIntervalMs) {
        String[] cameraIds = appProps.getProperty("camera.id", "cam1").split(",");
        String[] cameraUrls = appProps.getProperty("camera.url", "/app/videos/sample.mp4").split(",");

        if (cameraIds.length != cameraUrls.length) {
            throw new IllegalArgumentException("Number of camera IDs and URLs must match");
        }

        List<String> trimmedIds = Arrays.stream(cameraIds).map(String::trim).collect(Collectors.toList());
        try {
            assignmentManager.registerCameras(trimmedIds);
        } catch (Exception e) {
            log.error("Failed to register cameras", e);
            throw new RuntimeException("Failed to register cameras", e);
        }

        log.info("Starting {} camera stream threads...", cameraIds.length);

        for (int i = 0; i < cameraIds.length; i++) {
            String camId = cameraIds[i].trim();
            String url = cameraUrls[i].trim();
            executor.submit(new VideoEventCreator(camId, url, producer, assignmentManager, loadReportIntervalMs));
        }
    }

    private record TopicInfo(String baseTopic, String defaultTopic, long loadReportIntervalMs) {
        static TopicInfo fromProperties(Properties props) {
            String explicitBase = props.getProperty("kafka.topic.base");
            String fallback = props.getProperty("kafka.topic", "video-events");
            String inferredBase = explicitBase;
            if (inferredBase == null || inferredBase.isBlank()) {
                inferredBase = fallback.endsWith("-0") ? fallback.substring(0, fallback.length() - 2) : fallback;
            }
            String defaultTopic = inferredBase + "-0";
            long interval = Long.parseLong(props.getProperty("scaler.load.report.interval.ms", "2000"));
            return new TopicInfo(inferredBase, defaultTopic, interval);
        }
    }
}

