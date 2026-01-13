package com.ssau.scaler;

import java.util.Properties;

import com.ssau.scaler.service.TopicScaler;
import com.ssau.scaler.utils.ConfigLoader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicScalerApp {

    public static void main(String[] args) {
        try {
            Properties props = ConfigLoader.loadDefault();
            TopicScaler scaler = new TopicScaler(props);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down TopicScaler...");
                scaler.close();
            }));
            log.info("TopicScaler started. Awaiting shutdown signal...");
            scaler.awaitShutdown();
        } catch (Exception e) {
            log.error("Failed to start TopicScaler", e);
        }
    }
}
