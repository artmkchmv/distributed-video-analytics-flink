package com.ssau.processor.service;

import java.util.Collections;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

import com.ssau.processor.model.VideoFrameData;

@Slf4j
public class FrameProcessorHelper {
    
    public static VideoFrameData processMotionFrame(
            VideoFrameData frame,
            VideoFrameData previousProcessed,
            String outputDir) {
        try {
            Iterator<VideoFrameData> frameIterator = Collections.singletonList(frame).iterator();
            
            return MotionDetector.detectMotion(
                frame.getCamId(),
                frameIterator,
                outputDir,
                previousProcessed
            );
        } catch (Exception e) {
            log.error("Error processing motion frame for camera {}", frame.getCamId(), e);
            return null;
        }
    }
}
