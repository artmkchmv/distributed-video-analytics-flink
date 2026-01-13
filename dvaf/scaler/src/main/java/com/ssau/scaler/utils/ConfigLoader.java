package com.ssau.scaler.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConfigLoader {

    private static Properties props = null;

    private ConfigLoader() {}

    public static synchronized Properties loadDefault() throws IOException {
        // Try local properties first (for running on PC), then default (for Docker)
        try {
            Properties localProps = load("scaler.local.properties");
            if (localProps != null && !localProps.isEmpty()) {
                return localProps;
            }
        } catch (IOException e) {
            log.debug("scaler.local.properties not found, trying scaler.properties");
        }
        return load("scaler.properties");
    }

    public static synchronized Properties load(String fileName) throws IOException {
        if (props == null) {
            props = new Properties();
            
            // First, try to load from external file (current directory or config directory)
            // This allows changing config without rebuilding
            Path externalPath = Paths.get(fileName);
            if (!Files.exists(externalPath)) {
                externalPath = Paths.get("config", fileName);
            }
            
            if (Files.exists(externalPath) && Files.isRegularFile(externalPath)) {
                try (FileInputStream fis = new FileInputStream(externalPath.toFile())) {
                    props.load(fis);
                    log.info("Configuration loaded from external file: {}", externalPath.toAbsolutePath());
                    return props;
                } catch (IOException ex) {
                    log.warn("Failed to load configuration from external file: {}, trying classpath", externalPath, ex);
                }
            }
            
            // Fallback to classpath (inside JAR)
            try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
                if (input == null) {
                    throw new IOException("Configuration file '" + fileName + "' not found in classpath or external directory");
                }
                props.load(input);
                log.info("Configuration loaded from classpath: {}", fileName);
            } catch (IOException ex) {
                log.error("Error loading configuration file '{}': {}", fileName, ex.getMessage());
                throw ex;
            }
        }
        return props;
    }
}

