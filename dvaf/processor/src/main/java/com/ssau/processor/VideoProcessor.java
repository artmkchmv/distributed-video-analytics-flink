package com.ssau.processor;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.extern.slf4j.Slf4j;

import com.ssau.processor.model.ProcessingResult;
import com.ssau.processor.model.VideoFrameData;
import com.ssau.processor.service.FrameProcessorHelper;
import com.ssau.processor.sink.CassandraSinkFunction;
import com.ssau.processor.utils.ConfigLoader;

@Slf4j
public class VideoProcessor {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static void main(String[] args) throws Exception {
        Properties props = ConfigLoader.loadDefault();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        String checkpointDir = props.getProperty("flink.checkpoint.dir", "file:///tmp/flink-checkpoint");
        String checkpointDirForCreation = checkpointDir.replaceFirst("^file:///?", "");
        if (checkpointDirForCreation.startsWith("./")) {
            checkpointDirForCreation = Paths.get(checkpointDirForCreation).toAbsolutePath().normalize().toString();
        }
        try {
            Files.createDirectories(Paths.get(checkpointDirForCreation));
        } catch (Exception e) {
            log.warn("Failed to create checkpoint directory: {}", checkpointDirForCreation, e);
        }
        
        env.enableCheckpointing(Long.parseLong(props.getProperty("flink.checkpoint.interval.ms", "60000")));
        if (!checkpointDir.startsWith("file://")) {
            checkpointDir = "file://" + checkpointDir;
        }
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        
        int parallelism = Integer.parseInt(props.getProperty("flink.parallelism", "2"));
        env.setParallelism(parallelism);

        String processedImageDir = props.getProperty("processed.output.dir", "/app/videos/processed-data");
        String processingMode = props.getProperty("processing.mode", "MOTION").toUpperCase();

        KafkaSource<String> kafkaSource = buildKafkaSource(props);
        
        DataStream<String> kafkaStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        DataStream<VideoFrameData> frameStream = kafkaStream
            .map(new JsonToFrameMapper())
            .name("Parse JSON to VideoFrameData")
            .filter(frame -> frame != null)
            .name("Filter Invalid Frames");

        KeyedStream<VideoFrameData, String> keyedStream = frameStream
            .keyBy(VideoFrameData::getCamId);

        DataStream<ProcessingResult> resultStream;
        resultStream = keyedStream
            .process(new FrameProcessorFunction(processedImageDir, processingMode))
            .name("Process Frames with Motion Detection");

        Properties cassandraProps = new Properties();
        cassandraProps.setProperty("cassandra.contact.point", props.getProperty("cassandra.contact.point", "localhost"));
        cassandraProps.setProperty("cassandra.port", props.getProperty("cassandra.port", "9042"));
        cassandraProps.setProperty("cassandra.keyspace", props.getProperty("cassandra.keyspace", "dvaf"));
        
        resultStream.addSink(new CassandraSinkFunction(cassandraProps))
            .name("Cassandra Sink");

        log.info("Starting Flink video processing job...");
        env.execute("Video Processing Job");
    }

    private static KafkaSource<String> buildKafkaSource(Properties props) {
        String topicBase = props.getProperty("kafka.topic.base");
        String explicitTopic = props.getProperty("kafka.topic");
        String bootstrapServers = props.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String offsetStrategy = props.getProperty("kafka.offset.strategy", "latest").toLowerCase();
        
        OffsetsInitializer offsetInitializer = "earliest".equals(offsetStrategy) 
            ? OffsetsInitializer.earliest() 
            : OffsetsInitializer.latest();
        
        KafkaSource<String> source;
        
        if (topicBase != null && !topicBase.isBlank()) {
            java.util.regex.Pattern topicPattern = java.util.regex.Pattern.compile(topicBase + "-.*");
            source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(topicPattern)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setStartingOffsets(offsetInitializer)
                .build();
            log.info("Subscribing to Kafka topics matching pattern: {} with offset strategy: {}", 
                topicBase + "-.*", offsetStrategy);
        } else if (explicitTopic != null && !explicitTopic.isBlank()) {
            source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(explicitTopic)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setStartingOffsets(offsetInitializer)
                .build();
            log.info("Subscribing to Kafka topic: {} with offset strategy: {}", explicitTopic, offsetStrategy);
        } else {
            throw new IllegalStateException("Either kafka.topic.base or kafka.topic must be set");
        }
        
        return source;
    }

    private static class JsonToFrameMapper extends RichMapFunction<String, VideoFrameData> {
        @Override
        public VideoFrameData map(String json) throws Exception {
            try {
                return objectMapper.readValue(json, VideoFrameData.class);
            } catch (Exception e) {
                log.error("Failed to parse JSON", e);
                return null;
            }
        }
    }

    private static class FrameProcessorFunction 
            extends KeyedProcessFunction<String, VideoFrameData, ProcessingResult> {
        
        private transient ValueState<VideoFrameData> previousFrameState;
        private final String outputDir;
        private final String processingMode;
        
        public FrameProcessorFunction(String outputDir, String processingMode) {
            this.outputDir = outputDir;
            this.processingMode = processingMode;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<VideoFrameData> descriptor = 
                new ValueStateDescriptor<>("previousFrame", TypeInformation.of(VideoFrameData.class));

            previousFrameState = getRuntimeContext().getState(descriptor);
            log.info("Motion detector will be used (no initialization needed)");
        }
        
        @Override
        public void processElement(VideoFrameData frame, Context ctx, Collector<ProcessingResult> out) throws Exception {
            VideoFrameData previousProcessed = previousFrameState.value();
            VideoFrameData processed = null;
            String detectionType = "motion";
            
            processed = FrameProcessorHelper.processMotionFrame(frame, previousProcessed, outputDir);
            
            if (processed != null) {
                previousFrameState.update(processed);
                
                ProcessingResult result = new ProcessingResult(
                    UUID.randomUUID(),
                    frame.getCamId(),
                    frame.getTimestamp(),
                    Instant.now(),
                    detectionType,
                    1,
                    frame.getRows(),
                    frame.getCols(),
                    String.format("%s/%s-T-%d.png", outputDir, frame.getCamId(), 
                                 frame.getTimestamp().toEpochMilli()),
                    null
                );
                
                out.collect(result);
            }
        }
    }
}
