package com.atscale.java.executors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unused", "RV_RETURN_VALUE_IGNORED"})
public abstract class SequentialSimulationExecutor<T> extends SimulationExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequentialSimulationExecutor.class);
    private static final String WORKING_DIR = "working_dir";
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    
    // Add stop control method - matching the concurrent version
    protected boolean shouldContinueSimulation() {
        File stopFile = new File(WORKING_DIR + "/control/stop_simulation");
        if (stopFile.exists() && !stopRequested.get()) {
            LOGGER.info("🛑 Stop signal detected. Gracefully stopping simulation...");
            stopRequested.set(true);
            try {
                File archivedFile = new File(WORKING_DIR + "/control/stop_simulation_" + System.currentTimeMillis());
                if (stopFile.renameTo(archivedFile)) {
                    LOGGER.info("📁 Stop signal archived to: " + archivedFile.getName());
                } else {
                    LOGGER.warn("Failed to archive stop file, deleting instead");
                    stopFile.delete();
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to archive stop file: " + e.getMessage());
                stopFile.delete();
            }
            return false;
        }
        return !stopRequested.get();
    }

    protected void execute() {
        try {
            // This assumes that the Maven wrapper script (mvnw) is present in the project root directory
            String projectRoot = getApplicationDirectory();
            String mavenScript = getMavenWrapperScript();

            // Get the Gatling simulation tasks and run each simulation in a separate JVM Process
            // that means we have the capability to run multiple simulations sequentially thus passing
            // params via -D system properties is safe and isolated per process
            List<MavenTaskDto<T>> tasks = getSimulationTasks();

            // Start a stop signal monitor thread
            Thread stopMonitorThread = new Thread(() -> {
                while (!stopRequested.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(2000); // Check every 2 seconds
                        if (new File(WORKING_DIR + "/control/stop_simulation").exists()) {
                            shouldContinueSimulation(); // This will trigger the stop
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            stopMonitorThread.setDaemon(true);
            stopMonitorThread.start();

            // Check stop signal before starting any tasks
            if (!shouldContinueSimulation()) {
                LOGGER.info("🛑 Simulation cancelled by stop signal before starting any tasks");
                stopMonitorThread.interrupt();
                return;
            }

            for (MavenTaskDto<T> task : tasks) {
                // Check stop signal before each task
                if (!shouldContinueSimulation()) {
                    LOGGER.info("🛑 Simulation cancelled by stop signal between tasks");
                    break;
                }
                
                String osName = System.getProperty("os.name").toLowerCase();
                LOGGER.info("OS is {}", osName);
                LOGGER.info("Running task: {}", task.getTaskName());
                LOGGER.info("Maven Command: {}", task.getMavenCommand());
                LOGGER.info("Simulation Class: {}", task.getSimulationClass());
                LOGGER.info("Run Description: {}", task.getRunDescription());

                Process process = null;
                try {
                    List<String> command = new java.util.ArrayList<>();
                    command.add(mavenScript);

                    // Add -Dgatling.simulationClass and -Dgatling.runDescription (no extra quotes)
                    String simClass = String.format("-D%s=%s", MavenTaskDto.GATLING_SIMULATION_CLASS, task.getSimulationClass());
                    String runDesc = String.format("-D%s=%s", MavenTaskDto.GATLING_RUN_DESCRIPTION, task.getRunDescription());
                    String model = String.format("-D%s=%s", MavenTaskDto.ATSCALE_MODEL, task.getModel());
                    String runId = String.format("-D%s=%s", MavenTaskDto.ATSCALE_RUN_ID, task.getRunId());
                    String logFileName = String.format("-D%s=%s", MavenTaskDto.ATSCALE_LOG_FILE_NAME, task.getRunLogFileName());
                    String logAppend = String.format("-D%s=%s", MavenTaskDto.GATLING_RUN_LOGAPPEND, task.isRunLogAppend());
                    String injectionSteps = String.format("-D%s=%s", MavenTaskDto.GATLING_INJECTION_STEPS, task.getInjectionSteps());
                    String ingestFile = String.format("-D%s=%s", MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE, task.getIngestionFileName());
                    String ingestFileHasHeader = String.format("-D%s=%s", MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE_HAS_HEADER, task.getIngestionFileHasHeader());
                    String additionalProperties = String.format("-D%s=%s", MavenTaskDto.ADDITIONAL_PROPERTIES, task.getAdditionalProperties());
                    String alternatePropertiesFileName = task.getAlternatePropertiesFileName();
                    
                    if(StringUtils.isNotEmpty(alternatePropertiesFileName)){
                        throw new UnsupportedOperationException("Sequential executors do not support the use of alternate properties files.  Remove the call(s) to setAlternatePropertiesFileName() in the task definition(s).");
                    }

                    LOGGER.debug("SimEx Using simulation class: {}", simClass);
                    LOGGER.debug("SimEx Using run description: {}", runDesc);
                    LOGGER.debug("SimEx Using model: {}", model);
                    LOGGER.debug("SimEx Using run id: {}", runId);
                    LOGGER.debug("SimEx Using log file name: {}", logFileName);
                    LOGGER.debug("SimEx Logging as append: {}", logAppend);
                    LOGGER.debug("SimEx Using injection steps: {}", injectionSteps);
                    LOGGER.debug("SimEx Using ingestion file: {}", ingestFile);
                    LOGGER.debug("SimEx Ingestion file has header: {}", ingestFileHasHeader);
                    LOGGER.debug("SimEx Using additional properties: {}", additionalProperties);

                    command.add(simClass);
                    command.add(runDesc);
                    command.add(model);
                    command.add(runId);
                    command.add(logFileName);
                    command.add(logAppend);
                    command.add(injectionSteps);
                    command.add(ingestFile);
                    command.add(ingestFileHasHeader);
                    command.add(additionalProperties);

                    // Add the Maven goal (e.g., gatling:test)
                    command.add(task.getMavenCommand());

                    ProcessBuilder processBuilder = new ProcessBuilder(command);
                    // Set working directory to project root where mvnw(.cmd) exists
                    processBuilder.directory(new File(projectRoot));
                    processBuilder.inheritIO(); // This will print output to console

                    LOGGER.info("Starting the test suite on a separate JVM.  Using command args: {}", command);
                    process = processBuilder.start();
                    
                    // Monitor the process with frequent stop checks
                    boolean processCompleted = false;
                    while (!processCompleted && !stopRequested.get()) {
                        try {
                            // Use waitFor with timeout to check frequently
                            if (process.waitFor(1, java.util.concurrent.TimeUnit.SECONDS)) {
                                processCompleted = true;
                            }
                            // Check for stop signal every second
                            if (!shouldContinueSimulation()) {
                                LOGGER.info("🛑 Stop requested while waiting for task: {}", task.getTaskName());
                                break;
                            }
                        } catch (InterruptedException e) {
                            LOGGER.info("🛑 Thread interrupted for task: {}", task.getTaskName());
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    // If stop was requested and process is still running, kill it
                    if (stopRequested.get() && process.isAlive()) {
                        LOGGER.info("🛑 Stopping task {} due to stop signal", task.getTaskName());
                        process.destroy();
                        if (process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
                            process.destroyForcibly();
                        }
                    }

                    if (process.exitValue() != 0 && !stopRequested.get()) {
                        LOGGER.error("Task {} failed with exit code: {}", task.getTaskName(), process.exitValue());
                        throw new RuntimeException("Task failed with exit code: " + process.exitValue());
                    } else if (stopRequested.get()) {
                        LOGGER.info("🛑 Task {} was stopped by user request", task.getTaskName());
                    } else {
                        LOGGER.info("Task {} completed successfully.", task.getTaskName());
                    }

                } catch (IOException e) {
                    LOGGER.error("IOException running task {}: {}", task.getTaskName(), e.getMessage());
                    if (!stopRequested.get()) {
                        throw new RuntimeException("Failed to run task: " + task.getTaskName(), e);
                    }
                } catch (InterruptedException e) {
                    LOGGER.info("🛑 Task {} was interrupted", task.getTaskName());
                    Thread.currentThread().interrupt();
                    // Ensure process is cleaned up if interrupted
                    if (process != null && process.isAlive()) {
                        process.destroy();
                    }
                } finally {
                    // Ensure process is cleaned up
                    if (process != null && process.isAlive()) {
                        process.destroy();
                    }
                }
            }
            
            // Stop the monitor thread
            stopMonitorThread.interrupt();
            
        } finally {
            // Because of the way logging initializes early it produces some empty log files
            // Delete all zero-byte files in the run_logs directory
            String runLogPath = Paths.get(getApplicationDirectory(), "run_logs").toString();
            LOGGER.info("Run Log Path: {}",  runLogPath);

            org.apache.logging.log4j.LogManager.shutdown();

            File runLogsDir = new File(runLogPath);
            if (runLogsDir.exists() && runLogsDir.isDirectory()) {
                File[] files = runLogsDir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile() && file.length() == 0) {
                            //noinspection ResultOfMethodCallIgnored
                            file.delete();
                        }
                    }
                }
            }
        }
    }

    protected abstract List<MavenTaskDto<T>> getSimulationTasks();
}