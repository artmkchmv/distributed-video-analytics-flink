# Distributed Video Analytics System

## Overview

This is a distributed video analytics system built with Java, Apache Flink, Apache Kafka, Apache Cassandra, and OpenCV. It is designed to process live video streams in real-time, detect motion, and store processing results.

The system consists of three main modules:

- **producer-node** – reads video frames using OpenCV, encodes them to JSON, and sends to Kafka. Manages camera registration in ZooKeeper and tracks topic assignments.

- **processor-node** – consumes frames from Kafka using Apache Flink, processes them with motion detection, and stores results in Cassandra and filesystem.

- **scaler-node** – manages dynamic scaling of Kafka topics: creates new topics as needed and distributes cameras across topics based on load.

## Architecture

```
Video Files → Producer → Kafka (scaled topics) → Flink Processor → Cassandra + Filesystem
                    ↓
              ZooKeeper ← Scaler Node
```

## Features

- Real-time frame streaming using Apache Kafka.

- JSON-based video frame serialization.

- Distributed video processing with Apache Flink.

- Motion detection using OpenCV.

- Dynamic topic scaling based on camera load.

- Coordination via ZooKeeper for camera-to-topic assignments.

- Persistent storage in Cassandra and filesystem.

## Technologies

| Technology / Library            | Version             | Purpose                                                                 |
|--------------------------------|---------------------|-------------------------------------------------------------------------|
| **Apache Kafka**               | 3.7.0               | Transmission of video frames between system components                 |
| **Apache Zookeeper**           | 3.8.4 (via Docker)  | Coordination of Kafka brokers and camera-to-topic assignments          |
| **Apache Flink Streaming**     | 1.19.0              | Core infrastructure for distributed stream processing                  |
| **Apache Flink Kafka Connector** | 3.2.0-1.19        | Integration of Kafka data into Flink Streaming                         |
| **Apache Cassandra**           | 4.1 (via Docker)    | Persistent storage for processing results                              |
| **OpenCV (Bytedeco)**          | 4.10.0-1.5.11       | Computer vision and motion detection                                   |
| **Jackson Databind**           | 2.15.2              | JSON serialization/deserialization of objects                          |
| **Jackson JSR310 Datatype**    | 2.15.2              | Serialization/deserialization of Java time types (e.g., `Instant`)     |
| **Apache Curator**             | 5.6.0               | ZooKeeper client library for coordination                             |
| **SLF4J API**                  | 2.0.17              | Logging abstraction                                                     |
| **Logback Classic**            | 1.5.18              | Concrete implementation for logging (with SLF4J)                        |

## Getting Started

### Requirements

- Java 17 (JDK)

- Apache Maven 3.6+

- Docker Desktop

### Environment Setup (Windows)

Before running the project, make sure to configure the following system environment variables:

- JAVA_HOME → path to your JDK (e.g., C:\Program Files\Java\jdk-17)

- MAVEN_HOME → path to Maven (e.g., C:\Program Files\Apache\Maven)

Add %JAVA_HOME%\bin and %MAVEN_HOME%\bin to your system PATH.

### Setup and Running on Windows

1. Clone the repository:

```bash
git clone <repository-url>
cd distributed-video-analytics-flink/dvaf
```

2. Build all modules:

```bash
mvn clean package
```

3. Launch infrastructure services (Kafka, Zookeeper, Flink, Cassandra) via Docker Compose:

```bash
docker-compose up -d
```

4. Specify the path to video streams in the `producer/src/main/resources/producer.properties` file:

```properties
camera.id=cam1,cam2
camera.url=videos/sample.mp4,videos/sample1.mp4
```

5. Run applications locally using the provided script (Windows):

```bash
start-apps.bat
```

Or run manually:

- Run `TopicScalerApp.java` (Scaler Node)
- Run `VideoProducer.java` (Producer Node)
- Run `VideoProcessor.java` (Processor Node / Flink Job)

### Special JVM Arguments for Flink on Windows

When running or debugging processor-node (Flink) locally, add the following VM arguments:

```
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
```

These are already included in the `start-apps.bat` script.

## Configuration

### Producer (`producer/src/main/resources/producer.properties`)

```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic.base=video-events
zookeeper.connect=localhost:2181
zookeeper.root.path=/dvaf
scaler.load.report.interval.ms=2000
camera.id=cam1,cam2
camera.url=videos/sample.mp4,videos/sample1.mp4
```

### Scaler (`scaler/src/main/resources/scaler.properties`)

```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic.base=video-events
kafka.topic.partitions=3
kafka.topic.replication.factor=1
zookeeper.connect=localhost:2181
zookeeper.root.path=/dvaf
scaler.max.cameras.per.topic=4
scaler.min.topics=1
```

### Processor (`processor/src/main/resources/processor.properties`)

```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic.base=video-events
kafka.offset.strategy=earliest
processed.output.dir=videos/processed-data
cassandra.contact.point=localhost
cassandra.port=9042
cassandra.keyspace=dvaf
flink.checkpoint.dir=file:///tmp/flink-checkpoint
flink.checkpoint.interval.ms=60000
flink.parallelism=2
```

**Note:** Processor subscribes to all topics matching pattern `video-events-.*`, allowing automatic processing of frames from all created topics.

## Monitoring

- Check logs in each application window (when running locally)
- Processed images: `videos/processed-data/`
- Results in Cassandra:
  ```bash
  docker exec -it cassandra cqlsh
  USE dvaf;
  SELECT * FROM processing_results LIMIT 10;
  ```

## Stopping the System

```bash
docker-compose down
```

Stop local applications by closing their respective windows or using Ctrl+C.
