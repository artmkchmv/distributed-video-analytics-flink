package com.ssau.producer.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicAssignmentManager implements Closeable {

    private final CuratorFramework curator;
    private final String camerasPath;
    private final String assignmentsPath;
    private final String defaultTopic;

    private final Map<String, CuratorCache> assignmentCaches = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<String>> cameraTopics = new ConcurrentHashMap<>();
    private final Set<String> registeredCameras = ConcurrentHashMap.newKeySet();

    public TopicAssignmentManager(String zkConnect,
                                  String rootPath,
                                  String defaultTopic) {
        Objects.requireNonNull(zkConnect, "zookeeper.connect must be provided");
        Objects.requireNonNull(rootPath, "zookeeper.root.path must be provided");
        Objects.requireNonNull(defaultTopic, "defaultTopic must be provided");

        this.camerasPath = rootPath + "/cameras";
        this.assignmentsPath = rootPath + "/assignments";
        this.defaultTopic = defaultTopic;
        this.curator = CuratorFrameworkFactory.builder()
            .connectString(zkConnect)
            .retryPolicy(new ExponentialBackoffRetry(1_000, 5))
            .build();
    }

    public void start() throws Exception {
        curator.start();
        curator.blockUntilConnected();
        ensureBasePaths();
        curator.getConnectionStateListenable().addListener(new ReRegisterOnReconnect());
    }

    public void registerCameras(Collection<String> cameraIds) throws Exception {
        for (String camId : cameraIds) {
            registerCamera(camId);
        }
    }

    public void registerCamera(String camId) throws Exception {
        Objects.requireNonNull(camId, "cameraId must be provided");
        registeredCameras.add(camId);
        createOrReplaceCameraNode(camId, "fps=0;ts=" + Instant.now().toEpochMilli());
        startAssignmentWatcher(camId);
    }

    public void reportCameraLoad(String camId, double fps) {
        String payload = String.format("fps=%.2f;ts=%d", fps, Instant.now().toEpochMilli());
        String cameraPath = cameraNodePath(camId);
        try {
            if (curator.checkExists().forPath(cameraPath) == null) {
                createOrReplaceCameraNode(camId, payload);
            } else {
                curator.setData().forPath(cameraPath, payload.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.debug("Failed to update camera load for {}: {}", camId, e.getMessage());
        }
    }

    public String topicFor(String camId) {
        return cameraTopics.computeIfAbsent(camId, key -> new AtomicReference<>(defaultTopic))
            .get();
    }

    private void ensureBasePaths() throws Exception {
        if (curator.checkExists().forPath(camerasPath) == null) {
            curator.create().creatingParentsIfNeeded().forPath(camerasPath);
        }
        if (curator.checkExists().forPath(assignmentsPath) == null) {
            curator.create().creatingParentsIfNeeded().forPath(assignmentsPath);
        }
    }

    private void createOrReplaceCameraNode(String camId, String payload) throws Exception {
        String path = cameraNodePath(camId);
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        if (curator.checkExists().forPath(path) != null) {
            curator.delete().forPath(path);
        }
        curator.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(path, data);
    }

    private void startAssignmentWatcher(String camId) throws Exception {
        cameraTopics.computeIfAbsent(camId, key -> new AtomicReference<>(defaultTopic));
        String path = assignmentsPath + "/" + camId;
        
        CuratorCache cache = CuratorCache.build(curator, path);
        CuratorCacheListener listener = (type, oldData, data) -> {
            String topic = defaultTopic;
            if (data != null && data.getData() != null) {
                topic = new String(data.getData(), StandardCharsets.UTF_8);
            }
            cameraTopics.get(camId).set(topic);
            log.info("Camera {} assigned to topic {}", camId, topic);
        };
        cache.listenable().addListener(listener);
        
        try {
            cache.start();
            Thread.sleep(100);
            Optional<ChildData> currentDataOpt = cache.get(path);
            if (currentDataOpt.isPresent()) {
                ChildData currentData = currentDataOpt.get();
                byte[] data = currentData.getData();
                if (data != null && data.length > 0) {
                    String topic = new String(data, StandardCharsets.UTF_8);
                    cameraTopics.get(camId).set(topic);
                    log.info("Camera {} initially assigned to topic {}", camId, topic);
                }
            }
        } catch (Exception e) {
            log.debug("Assignment node {} not available yet or error starting cache", path, e);
        }
        assignmentCaches.put(camId, cache);
    }

    private String cameraNodePath(String camId) {
        return camerasPath + "/" + camId;
    }

    @Override
    public void close() throws IOException {
        assignmentCaches.values().forEach(cache -> {
            try {
                cache.close();
            } catch (Exception e) {
                log.debug("Failed to close NodeCache", e);
            }
        });
        curator.close();
    }

    private class ReRegisterOnReconnect implements ConnectionStateListener {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            if (newState == ConnectionState.RECONNECTED) {
                log.info("Zookeeper connection re-established. Re-registering cameras {}", registeredCameras);
                for (String camId : registeredCameras) {
                    try {
                        createOrReplaceCameraNode(camId, "fps=0;ts=" + Instant.now().toEpochMilli());
                    } catch (Exception e) {
                        log.warn("Failed to re-register camera {}", camId, e);
                    }
                }
            }
        }
    }
}

