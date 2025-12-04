package com.atscale.java.executors;

import java.io.File;
import java.io.IOException;
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
                    LOGGER.info("📁 Stop signal archived to: " + archivedFile.getName());
                } else {
                    LOGGER.warn("Failed to archive stop file, deleting instead");
                    stopFile.delete();
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to archive stop file: " + e.getMessage());
                stopFile.delete();
            }
            // Stop all running processes immediately
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
                    LOGGER.info("Destroying process...");
                    process.destroy();
                    // Wait a bit for graceful shutdown
                    Thread.sleep(1000);
                    if (process.isAlive()) {
                        LOGGER.info("Forcibly destroying process...");
                        process.destroyForcibly();
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error stopping process: " + e.getMessage());
                }
            }
        }
        runningProcesses.clear();
    }

    protected void execute() {
        try {
            String projectRoot = getApplicationDirectory();
            String mavenScript = getMavenWrapperScript();

            // Check stop signal before starting
            if (!shouldContinueSimulation()) {
                LOGGER.info("🛑 Simulation cancelled by stop signal before starting");
                return;
            }

            List<Thread> taskThreads = new java.util.ArrayList<>();
            List<MavenTaskDto<T>> tasks = getSimulationTasks();

            // Start a stop signal monitor thread
            Thread stopMonitorThread = new Thread(() -> {
                while (!stopRequested.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(2000); // Check every 2 seconds
                        if (new File("working_dir/control/stop_simulation").exists()) {
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

            for (MavenTaskDto<T> task : tasks) {
                // Check stop signal before starting each task
                if (!shouldContinueSimulation()) {
                    LOGGER.info("🛑 Simulation cancelled by stop signal before starting task: {}", task.getTaskName());
                    break;
                }

                Thread taskThread = new Thread(() -> {
                    // Check stop signal at the beginning of each thread
                    if (!shouldContinueSimulation()) {
                        LOGGER.info("🛑 Task {} cancelled by stop signal", task.getTaskName());
                        return;
                    }

                    Process process = null;
                    try {
                        List<String> command = new java.util.ArrayList<>();
                        command.add(mavenScript);

                        // Add all the parameters
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
                        
                        if(StringUtils.isNotEmpty(task.getAlternatePropertiesFileName())){
                            command.add(String.format("-D%s=%s", "systems.properties.file", task.getAlternatePropertiesFileName()));
                        }

                        // Add the Maven goal
                        command.add(task.getMavenCommand());

                        ProcessBuilder processBuilder = new ProcessBuilder(command);
                        processBuilder.directory(new File(projectRoot));
                        processBuilder.inheritIO();

                        LOGGER.info("Starting task {} with command: {}", task.getTaskName(), command);
                        process = processBuilder.start();
                        runningProcesses.add(process);

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
                    } finally {
                        if (process != null) {
                            runningProcesses.remove(process);
                            // Ensure process is cleaned up
                            if (process.isAlive()) {
                                process.destroy();
                            }
                        }
                    }
                });
                
                taskThread.start();
                taskThreads.add(taskThread);
                
                // Small delay between starting threads to avoid overwhelming the system
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // Wait for all task threads to complete
            for (Thread taskThread : taskThreads) {
                try {
                    taskThread.join();
                } catch (InterruptedException e) {
                    LOGGER.info("🛑 Main thread interrupted while waiting for tasks");
                    Thread.currentThread().interrupt();
                    // Interrupt all running task threads
                    for (Thread t : taskThreads) {
                        if (t.isAlive()) {
                            t.interrupt();
                        }
                    }
                    break;
                }
            }
            
            // Stop the monitor thread
            stopMonitorThread.interrupt();
            
        } finally {
            // Cleanup
            stopAllProcesses();
            
            // Delete empty log files
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