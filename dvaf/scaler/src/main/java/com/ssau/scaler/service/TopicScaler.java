package com.ssau.scaler.service;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicScaler implements Closeable {

    private final CuratorFramework curator;
    private final AdminClient adminClient;
    private final PathChildrenCache cameraCache;
    private final ExecutorService reconcileExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "topic-scaler-reconciler");
        t.setDaemon(true);
        return t;
    });
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final String camerasPath;
    private final String assignmentsPath;
    private final String baseTopic;
    private final int topicPartitions;
    private final short replicationFactor;
    private final int maxCamerasPerTopic;
    private final int minTopicCount;

    public TopicScaler(Properties props) throws Exception {
        String zkConnect = props.getProperty("zookeeper.connect");
        Objects.requireNonNull(zkConnect, "zookeeper.connect must be provided");

        String bootstrapServers = props.getProperty("kafka.bootstrap.servers");
        Objects.requireNonNull(bootstrapServers, "kafka.bootstrap.servers must be provided");

        this.baseTopic = props.getProperty("kafka.topic.base", "video-events");
        this.topicPartitions = Integer.parseInt(props.getProperty("kafka.topic.partitions", "1"));
        this.replicationFactor = Short.parseShort(props.getProperty("kafka.topic.replication.factor", "1"));
        this.maxCamerasPerTopic = Math.max(1, Integer.parseInt(props.getProperty("scaler.max.cameras.per.topic", "4")));
        this.minTopicCount = Math.max(1, Integer.parseInt(props.getProperty("scaler.min.topics", "1")));

        String rootPath = props.getProperty("zookeeper.root.path", "/dvaf");
        this.camerasPath = rootPath + "/cameras";
        this.assignmentsPath = rootPath + "/assignments";

        this.curator = CuratorFrameworkFactory.newClient(
            zkConnect,
            new ExponentialBackoffRetry(1_000, 5));
        this.curator.start();

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        this.adminClient = AdminClient.create(adminProps);

        ensureBasePaths();

        this.cameraCache = new PathChildrenCache(curator, camerasPath, true);
        this.cameraCache.getListenable().addListener(new CameraChangeListener());
        this.cameraCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        reconcileExecutor.submit(() -> reconcileAssignments("initial startup"));
    }

    private void ensureBasePaths() throws Exception {
        if (curator.checkExists().forPath(camerasPath) == null) {
            curator.create().creatingParentsIfNeeded().forPath(camerasPath);
        }
        if (curator.checkExists().forPath(assignmentsPath) == null) {
            curator.create().creatingParentsIfNeeded().forPath(assignmentsPath);
        }
    }

    private void reconcileAssignments(String reason) {
        synchronized (this) {
            try {
                List<String> cameras = new ArrayList<>(curator.getChildren().forPath(camerasPath));
                Collections.sort(cameras);
                int requiredTopics = Math.max(minTopicCount,
                    (int) Math.ceil((double) cameras.size() / (double) maxCamerasPerTopic));
                ensureTopicCount(requiredTopics);
                applyAssignments(cameras, requiredTopics);
                log.info("Reconciled assignments due to {}. cameras={}, topics={}", reason, cameras.size(), requiredTopics);
            } catch (Exception e) {
                log.error("Failed to reconcile assignments", e);
            }
        }
    }

    private void ensureTopicCount(int requiredTopics) throws Exception {
        Set<String> desiredTopics = new HashSet<>();
        for (int i = 0; i < requiredTopics; i++) {
            desiredTopics.add(topicName(i));
        }

        Set<String> existingTopics;
        try {
            existingTopics = adminClient.listTopics().names().get(15, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Timed out while listing topics, continuing with empty cache", e);
            existingTopics = Collections.emptySet();
        } catch (ExecutionException e) {
            log.warn("Failed to list topics from Kafka, continuing with empty cache", e);
            existingTopics = Collections.emptySet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        List<NewTopic> newTopics = new ArrayList<>();
        for (String topic : desiredTopics) {
            if (!existingTopics.contains(topic)) {
                NewTopic newTopic = new NewTopic(topic, topicPartitions, replicationFactor);
                newTopic.configs(Collections.singletonMap("max.message.bytes", "10485760"));
                newTopics.add(newTopic);
            }
        }

        if (newTopics.isEmpty()) {
            return;
        }

        try {
            adminClient.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.debug("Topics already exist, skipping creation");
            } else {
                log.error("Create topics failed", e);
            }
        } catch (TimeoutException e) {
            log.error("Timed out while waiting for topic creation", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private void applyAssignments(List<String> cameras, int requiredTopics) throws Exception {
        Set<String> activeCameras = new HashSet<>(cameras);

        List<String> currentAssignmentNodes = curator.getChildren().forPath(assignmentsPath);
        for (String node : currentAssignmentNodes) {
            if (!activeCameras.contains(node)) {
                curator.delete().forPath(assignmentsPath + "/" + node);
            }
        }

        for (int i = 0; i < cameras.size(); i++) {
            String camId = cameras.get(i);
            int topicIndex = Math.min(requiredTopics - 1, i / maxCamerasPerTopic);
            String topic = topicName(topicIndex);
            byte[] payload = topic.getBytes(StandardCharsets.UTF_8);
            String path = assignmentsPath + "/" + camId;
            if (curator.checkExists().forPath(path) == null) {
                curator.create().creatingParentsIfNeeded().forPath(path, payload);
            } else {
                curator.setData().forPath(path, payload);
            }
        }
    }

    private String topicName(int idx) {
        return baseTopic + "-" + idx;
    }

    @Override
    public void close() {
        try {
            cameraCache.close();
        } catch (Exception e) {
            log.warn("Failed to close camera cache", e);
        }
        reconcileExecutor.shutdownNow();
        try {
            adminClient.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("Failed to close admin client", e);
        }
        curator.close();
        shutdownLatch.countDown();
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    private class CameraChangeListener implements PathChildrenCacheListener {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_REMOVED:
                    log.info("Camera registry changed: {} at {}", event.getType(), Instant.now());
                    reconcileExecutor.submit(() -> reconcileAssignments(event.getType().name()));
                    break;
                case CHILD_UPDATED:
                case CONNECTION_RECONNECTED:
                    log.debug("Camera event {}", event.getType());
                    break;
                default:
                    log.trace("Ignoring camera event {}", event.getType());
            }
        }
    }
}
