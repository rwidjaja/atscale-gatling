package com.atscale.java.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatlingErrorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(GatlingErrorHandler.class);
    
    // Call this method at the start of your sequential simulation
    public static void configureForSequentialExecution() {
        // Set system properties to help Gatling handle null messages
        System.setProperty("gatling.core.stats.writer.allowNullMessage", "true");
        System.setProperty("gatling.data.file.bufferSize", "65536");
        System.setProperty("gatling.core.directory.binaries", "target/gatling");
        System.setProperty("gatling.data.file.writer", "io.gatling.core.stats.writer.LogFileDataWriter");
        
        LOGGER.info("Configured Gatling for sequential execution");
    }
}