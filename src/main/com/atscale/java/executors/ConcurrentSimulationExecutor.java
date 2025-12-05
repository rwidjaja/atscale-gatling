package com.atscale.java.executors;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public abstract class ConcurrentSimulationExecutor<T> extends SimulationExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentSimulationExecutor.class);
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final List<Process> runningProcesses = new CopyOnWriteArrayList<>();

    protected boolean shouldContinueSimulation() {
        File stopFile = new File("working_dir/control/stop_simulation");
        if (stopFile.exists() && !stopRequested.get()) {
            LOGGER.info("🛑 Stop signal detected. Gracefully stopping simulation...");
            stopRequested.set(true);
            try {
                File archivedFile = new File("working_dir/control/stop_simulation_" + System.currentTimeMillis());
                if (stopFile.renameTo(archivedFile)) {
                    LOGGER.info("📁 Stop signal archived to: {}", archivedFile.getName());
                } else {
                    LOGGER.warn("Failed to archive stop file, deleting instead");
                    stopFile.delete();
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to archive stop file: {}", e.getMessage());
                stopFile.delete();
            }
            stopAllProcesses();
            return false;
        }
        return !stopRequested.get();
    }

    private void stopAllProcesses() {
        LOGGER.info("🛑 Stopping all running processes...");
        for (Process process : runningProcesses) {
            if (process.isAlive()) {
                try {
                    process.destroy();
                    if (process.waitFor(1, java.util.concurrent.TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error stopping process: {}", e.getMessage());
                }
            }
        }
        runningProcesses.clear();
    }

    protected void execute() {
        try {
            String projectRoot = getApplicationDirectory();
            String mavenScript = getMavenWrapperScript();

            if (!shouldContinueSimulation()) {
                LOGGER.info("🛑 Simulation cancelled before starting");
                return;
            }

            List<Thread> taskThreads = new java.util.ArrayList<>();
            List<MavenTaskDto<T>> tasks = getSimulationTasks();

            Thread stopMonitorThread = new Thread(() -> {
                while (!stopRequested.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(2000);
                        if (new File("working_dir/control/stop_simulation").exists()) {
                            shouldContinueSimulation();
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

            for (MavenTaskDto<T> task : tasks) {
                if (!shouldContinueSimulation()) {
                    LOGGER.info("🛑 Cancelled before starting task: {}", task.getTaskName());
                    break;
                }

                Thread taskThread = new Thread(() -> {
                    if (!shouldContinueSimulation()) {
                        LOGGER.info("🛑 Task {} cancelled by stop signal", task.getTaskName());
                        return;
                    }

                    Process process = null;
                    try {
                        List<String> command = new java.util.ArrayList<>();
                        command.add(mavenScript);
                        command.add("-q"); // quiet mode

                        // Add system properties
                        command.add(String.format("-D%s=%s", MavenTaskDto.GATLING_SIMULATION_CLASS, task.getSimulationClass()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.GATLING_RUN_DESCRIPTION, task.getRunDescription()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.ATSCALE_MODEL, task.getModel()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.ATSCALE_RUN_ID, task.getRunId()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.ATSCALE_LOG_FILE_NAME, task.getRunLogFileName()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.GATLING_RUN_LOGAPPEND, task.isRunLogAppend()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.GATLING_INJECTION_STEPS, task.getInjectionSteps()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE, task.getIngestionFileName()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE_HAS_HEADER, task.getIngestionFileHasHeader()));
                        command.add(String.format("-D%s=%s", MavenTaskDto.ADDITIONAL_PROPERTIES, task.getAdditionalProperties()));

                        if (StringUtils.isNotEmpty(task.getAlternatePropertiesFileName())) {
                            command.add(String.format("-D%s=%s", "systems.properties.file", task.getAlternatePropertiesFileName()));
                        }

                        // Add Maven goal
                        command.add(task.getMavenCommand());

                        ProcessBuilder processBuilder = new ProcessBuilder(command);
                        processBuilder.directory(new File(projectRoot));
                        processBuilder.redirectErrorStream(true);

                        LOGGER.info("Starting task {} with command: {}", task.getTaskName(), command);
                        process = processBuilder.start();
                        runningProcesses.add(process);

                        // Filter output
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                if (line.contains("GATLING") || line.contains("REQUEST") ||
                                    line.contains("SQL") || line.contains("XMLA")) {
                                    LOGGER.info("GATLING: {}", line);
                                }
                            }
                        }

                        // Poll for completion with stop checks
                        boolean processCompleted = false;
                        while (!processCompleted && !stopRequested.get()) {
                            if (process.waitFor(1, java.util.concurrent.TimeUnit.SECONDS)) {
                                processCompleted = true;
                            }
                            if (!shouldContinueSimulation()) {
                                LOGGER.info("🛑 Stop requested while waiting for task: {}", task.getTaskName());
                                break;
                            }
                        }

                        if (stopRequested.get() && process.isAlive()) {
                            LOGGER.info("🛑 Stopping task {} due to stop signal", task.getTaskName());
                            process.destroy();
                            if (process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
                                process.destroyForcibly();
                            }
                        }

                        int exitCode = process.exitValue();
                        if (exitCode != 0 && !stopRequested.get()) {
                            throw new RuntimeException("Task failed with exit code: " + exitCode);
                        } else if (stopRequested.get()) {
                            LOGGER.info("🛑 Task {} was stopped by user request", task.getTaskName());
                        } else {
                            LOGGER.info("Task {} completed successfully.", task.getTaskName());
                        }

                    } catch (IOException | InterruptedException e) {
                        LOGGER.error("Error running task {}: {}", task.getTaskName(), e.getMessage());
                        Thread.currentThread().interrupt();
                    } finally {
                        if (process != null) {
                            runningProcesses.remove(process);
                            if (process.isAlive()) {
                                process.destroy();
                            }
                        }
                    }
                });

                taskThread.start();
                taskThreads.add(taskThread);

                try {
                    Thread.sleep(1000); // small delay between threads
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            for (Thread taskThread : taskThreads) {
                try {
                    taskThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            stopMonitorThread.interrupt();

        } finally {
            stopAllProcesses();

            String runLogPath = Paths.get(getApplicationDirectory(), "run_logs").toString();
            LOGGER.info("Run Log Path: {}", runLogPath);

            org.apache.logging.log4j.LogManager.shutdown();

            File runLogsDir = new File(runLogPath);
            if (runLogsDir.exists() && runLogsDir.isDirectory()) {
                File[] files = runLogsDir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile() && file.length() == 0) {
                            file.delete();
                        }
                    }
                }
            }
        }
    }

    protected abstract List<MavenTaskDto<T>> getSimulationTasks();
}
