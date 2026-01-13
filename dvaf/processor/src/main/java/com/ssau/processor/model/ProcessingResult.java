package com.ssau.processor.model;

import java.time.Instant;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingResult {
    
    private UUID id;
    private String cameraId;
    private Instant frameTimestamp;
    private Instant processingTimestamp;
    private String detectionType;
    private int detectionCount;
    private int frameRows;
    private int frameCols;
    private String imagePath;
    private String metadata;
}

