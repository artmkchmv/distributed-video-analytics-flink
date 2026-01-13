package com.ssau.producer.service;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Base64;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bytedeco.opencv.global.opencv_imgproc;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Size;
import org.bytedeco.opencv.opencv_videoio.VideoCapture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.ssau.producer.model.VideoFrameData;
import com.ssau.producer.zookeeper.TopicAssignmentManager;

@Slf4j
@RequiredArgsConstructor
public class VideoEventCreator implements Runnable {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String cameraId;
    private final String url;
    private final Producer<String, String> producer;
    private final TopicAssignmentManager assignmentManager;
    private final long loadReportIntervalMs;
    private final int frameWidth = 640;
    private final int frameHeight = 480;

    @Override
    public void run() {
        log.info("OpenCV loaded, starting video processing");
        log.info("Processing cameraId {} with url {}", cameraId, url);
        try {
            createEvent();
        } catch (Exception e) {
            log.error("Error processing cameraId {}: ", cameraId, e);
        }
    }

    private void createEvent() throws Exception {
        VideoCapture camera = openCamera();
        if (!camera.isOpened()) {
            log.error("Camera not opened for cameraId={}, url={}", cameraId, url);
            return;
        }
        
        Mat mat = new Mat();
        if (!camera.read(mat)) {
            log.error("Failed to read first frame from cameraId={}, url={}", cameraId, url);
            return;
        }
        
        log.info("First frame read successfully");
        try {
            int frameCount = 0;
            long lastReportTs = System.currentTimeMillis();
            int framesSinceReport = 0;
            while (camera.read(mat)) {
                frameCount++;
                framesSinceReport++;
                opencv_imgproc.resize(mat, mat, new Size(frameWidth, frameHeight), 0, 0, opencv_imgproc.INTER_CUBIC);
                String json = createJsonFromMat(mat);
                sendMessage(json);
                Thread.sleep(33); // ~30 FPS
                long now = System.currentTimeMillis();
                if (now - lastReportTs >= loadReportIntervalMs) {
                    double fps = framesSinceReport * 1000d / (now - lastReportTs);
                    assignmentManager.reportCameraLoad(cameraId, fps);
                    framesSinceReport = 0;
                    lastReportTs = now;
                }
            }
            if (framesSinceReport > 0) {
                double fps = framesSinceReport * 1000d / Math.max(1, System.currentTimeMillis() - lastReportTs);
                assignmentManager.reportCameraLoad(cameraId, fps);
            }
            log.info("Finished reading video for cameraId={}, total frames={}", cameraId, frameCount);
        } finally {
            mat.release();
            camera.release();
        }
    }

    private VideoCapture openCamera() throws Exception {
        VideoCapture camera;
        
        if (url.matches("\\d+")) {
            int cameraIndex = Integer.parseInt(url);
            log.info("Opening camera index: {}", cameraIndex);
            camera = new VideoCapture(cameraIndex);
        } else {
            String resolvedPath = resolveVideoPath(url);
            log.info("Resolved video path: {} -> {}", url, resolvedPath);
            log.info("Current working directory: {}", System.getProperty("user.dir"));
            
            File file = new File(resolvedPath);
            log.info("File exists: {}, isFile: {}, absolute path: {}", 
                file.exists(), file.isFile(), file.getAbsolutePath());
            
            if (!file.exists()) {
                log.error("Video file does not exist at resolved path: {}", resolvedPath);
                throw new Exception("Video file not found: " + resolvedPath);
            }
            
            String opencvPath = resolvedPath.replace('\\', '/');
            log.info("Using OpenCV path: {}", opencvPath);
            camera = new VideoCapture(opencvPath);
        }

        if (!camera.isOpened()) {
            log.warn("Failed to open camera/video on first attempt for cameraId={}, url={}", cameraId, url);
            Thread.sleep(5000);
            if (!camera.isOpened()) {
                if (url.matches("\\d+")) {
                    camera.open(Integer.parseInt(url));
                } else {
                    String resolvedPath = resolveVideoPath(url);
                    String opencvPath = resolvedPath.replace('\\', '/');
                    log.info("Retrying to open video file: {}", opencvPath);
                    camera.open(opencvPath);
                }
            }
            
            if (!camera.isOpened()) {
                log.error("Failed to open camera/video after retry for cameraId={}, url={}", cameraId, url);
                throw new Exception("Error opening cameraId " + cameraId + " with url=" + url + ". Check camera URL/path.");
            } else {
                log.info("Camera/video opened successfully after retry for cameraId={}, url={}", cameraId, url);
            }
        } else {
            log.info("Camera/video opened successfully on first attempt for cameraId={}, url={}", cameraId, url);
        }

        return camera;
    }

    private String resolveVideoPath(String path) {
        // Check absolute paths (Windows: C:\, Linux: /)
        boolean isAbsolute = new File(path).isAbsolute() || path.startsWith("/");
        if (isAbsolute || path.startsWith("file://") || path.startsWith("http://") || path.startsWith("https://")) {
            File absFile = new File(path);
            if (absFile.exists() && absFile.isFile()) {
                log.debug("Using absolute path as-is: {}", path);
                return path.replace('\\', '/');
            } else {
                log.warn("Absolute path does not exist: {}, will try to resolve", path);
            }
        }
        
        Path currentDir = Paths.get("").toAbsolutePath();
        Path directPath = currentDir.resolve(path).normalize();
        File directFile = directPath.toFile();
        if (directFile.exists() && directFile.isFile()) {
            log.debug("Found file using direct path: {} -> {}", path, directPath.toAbsolutePath());
            return directPath.toAbsolutePath().toString().replace('\\', '/');
        }
        
        String[] possibleBases = {
            "",
            "dvaf",
            "..",
            "../dvaf",
            "../../dvaf"
        };
        
        for (String base : possibleBases) {
            Path basePath = base.isEmpty() ? currentDir : currentDir.resolve(base).normalize();
            Path resolved = basePath.resolve(path).normalize();
            File file = resolved.toFile();
            if (file.exists() && file.isFile()) {
                log.debug("Found file using base '{}': {} -> {}", base, path, resolved.toAbsolutePath());
                return resolved.toAbsolutePath().toString().replace('\\', '/');
            }
        }
        
        String[] videoDirs = {
            "dvaf/videos",
            "../videos",
            "../../videos",
            "videos",
            "/app/videos"
        };
        
        String filename = Paths.get(path).getFileName().toString();
        
        for (String videoDir : videoDirs) {
            Path resolved;
            if (videoDir.startsWith("/")) {
                resolved = Paths.get(videoDir).resolve(filename).normalize();
            } else {
                resolved = currentDir.resolve(videoDir).resolve(filename).normalize();
            }
            File file = resolved.toFile();
            if (file.exists() && file.isFile()) {
                log.debug("Found file in video directory '{}': {} -> {}", videoDir, path, resolved.toAbsolutePath());
                return resolved.toAbsolutePath().toString().replace('\\', '/');
            }
        }
        
        log.warn("Could not resolve video path: {} (current dir: {}), using as-is", path, currentDir);
        return path.replace('\\', '/');
    }

    private String createJsonFromMat(Mat mat) throws Exception {
        int size = (int) (mat.total() * mat.channels());
        byte[] data = new byte[size];
        mat.data().get(data);

        VideoFrameData event = new VideoFrameData(
            cameraId,
            Instant.now(),
            mat.rows(),
            mat.cols(),
            mat.type(),
            Base64.getEncoder().encodeToString(data)
        );

        return objectMapper.writeValueAsString(event);
    }

    private void sendMessage(String json) {
        String topic = assignmentManager.topicFor(cameraId);
        producer.send(new ProducerRecord<>(topic, cameraId, json), (RecordMetadata rm, Exception ex) -> {
            if (rm != null) {
                log.debug("Message sent for cameraId={} to topic={} partition={}", cameraId, topic, rm.partition());
            }
            if (ex != null) {
                log.error("Error sending message for cameraId={}", cameraId, ex);
            }
        });
    }
}

