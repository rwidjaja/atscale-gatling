package executors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.atscale.java.executors.ConcurrentSimulationExecutor;
import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.injectionsteps.ClosedStep;  // Change this import
import com.atscale.java.injectionsteps.ConstantConcurrentUsersClosedInjectionStep;

public class ClosedStepSequentialSimulationExecutor extends ConcurrentSimulationExecutor<ClosedStep> {  // Extend ConcurrentSimulationExecutor instead
    
    private static final Logger logger = Logger.getLogger(ClosedStepSequentialSimulationExecutor.class.getName());
    private static final String WORKING_DIR = "working_dir";
    private static final String QUERIES_DIR = "queries";
    private static final String INGEST_DIR = "ingest";
    private static final String CONFIG_DIR = "config";
    
    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", 
                          "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }
    
    public static void main(String[] args) {
        logger.info("ClosedStepSequentialSimulationExecutor started.");
        
        ClosedStepSequentialSimulationExecutor executor = new ClosedStepSequentialSimulationExecutor();
        executor.execute();  // This will use ConcurrentSimulationExecutor's execute method
        
        logger.info("ClosedStepSequentialSimulationExecutor completed.");
    }
    
    @Override
    protected List<MavenTaskDto<ClosedStep>> getSimulationTasks() {
       List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        
        try {
            Map<String, String> connectionDetails = loadConnectionDetails();
            if (connectionDetails.isEmpty()) {
                logger.severe("Failed to load connection details from config.json");
                return tasks;
            }
            
            Properties systemsProps = loadSystemsProperties();
            Map<String, CubeConfig> cubeConfigs = getCubeConfigurations(systemsProps);
            
            if (cubeConfigs.isEmpty()) {
                logger.severe("No cube configurations found in systems.properties");
                return tasks;
            }
            
            logger.info("Processing " + cubeConfigs.size() + " cube configurations");
            
            for (Map.Entry<String, CubeConfig> cubeEntry : cubeConfigs.entrySet()) {
                String cubeIdentifier = cubeEntry.getKey();
                CubeConfig config = cubeEntry.getValue();
                
                logger.info("Processing cube - Identifier: " + cubeIdentifier + 
                           ", Cube: " + config.cubeName + 
                           ", Catalog: " + config.catalogName);
                
                tasks.addAll(createTasksForCube(cubeIdentifier, config.cubeName, config.catalogName, 
                                               connectionDetails, systemsProps));
            }
            
            logger.info("Created " + tasks.size() + " tasks for " + cubeConfigs.size() + " cubes");
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error loading tasks", e);
        }
        
        return tasks;
    }
    
   // Override the execute method to run tasks sequentially instead of concurrently
    @Override
    public void execute() {
        logger.info("=== Running tasks SEQUENTIALLY ===");
        
        List<MavenTaskDto<ClosedStep>> tasks = getSimulationTasks();
        
        if (tasks.isEmpty()) {
            logger.warning("No tasks to execute!");
            return;
        }
        
        // Execute tasks sequentially
        for (MavenTaskDto<ClosedStep> task : tasks) {
            try {
                logger.info("=== Executing task sequentially: " + task.getTaskName());
                
                // Use the parent class's single task execution
                // We need to check if there's a protected method we can call
                // If not, we'll need to implement our own execution
                executeSingleTask(task);
                
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to execute task: " + task.getTaskName(), e);
            }
        }
        
        logger.info("=== All tasks completed ===");
    }

    // Add this method if it doesn't exist in the parent class
    private void executeSingleTask(MavenTaskDto<ClosedStep> task) {
        try {
            List<String> command = new ArrayList<>();
            command.add("mvn");
            command.add(task.getMavenCommand());

            // Standard Gatling system properties
            command.add("-D" + MavenTaskDto.GATLING_SIMULATION_CLASS + "=" + task.getSimulationClass());
            command.add("-D" + MavenTaskDto.ATSCALE_MODEL + "=" + task.getModel());
            command.add("-D" + MavenTaskDto.GATLING_RUN_ID + "=" + task.getRunId());
            command.add("-D" + MavenTaskDto.GATLING_RUN_DESCRIPTION + "=" + task.getRunDescription());
            command.add("-D" + MavenTaskDto.GATLING_INJECTION_STEPS + "=" + task.getInjectionSteps());
            command.add("-D" + MavenTaskDto.GATLING_RUN_LOGFILENAME + "=" + task.getRunLogFileName());
            command.add("-D" + MavenTaskDto.GATLING_RUN_LOGAPPEND + "=" + task.isRunLogAppend());

            // ⭐ Correct location to insert the fix ⭐
            String additional = task.getAdditionalProperties();
            if (additional != null && !additional.isEmpty()) {
                command.add("-D" + MavenTaskDto.ADDITIONAL_PROPERTIES + "=" + additional);
            }

            // SSL config
            command.add("-Djavax.net.ssl.trustStore=./cacerts");
            command.add("-Djavax.net.ssl.trustStorePassword=changeit");
            command.add("-Djavax.net.ssl.keyStore=./cacerts");
            command.add("-Djavax.net.ssl.keyStorePassword=changeit");
            command.add("-Dsun.security.ssl.allowUnsafeRenegotiation=true");
            logger.info("Executing command: " + String.join(" ", command));

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.directory(new File("."));
            pb.redirectErrorStream(true);

            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.info("GATLING: " + line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode == 0) {
                logger.info("Task completed successfully: " + task.getTaskName());
            } else {
                logger.warning("Task exited with code " + exitCode + ": " + task.getTaskName());
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error executing task: " + task.getTaskName(), e);
            throw new RuntimeException("Failed to execute task: " + task.getTaskName(), e);
        }
    }

    private Map<String, CubeConfig> getCubeConfigurations(Properties systemsProps) {
        Map<String, CubeConfig> cubeConfigs = new HashMap<>();
        
        for (String propertyName : systemsProps.stringPropertyNames()) {
            if (propertyName.startsWith("atscale.") && propertyName.contains(".xmla.cube")) {
                // Extract cube identifier (the part between "atscale." and ".xmla.cube")
                String cubeIdentifier = propertyName.substring("atscale.".length(), propertyName.indexOf(".xmla.cube"));
                String cubeName = systemsProps.getProperty(propertyName);
                
                // Get the catalog name for this cube
                String catalogProperty = "atscale." + cubeIdentifier + ".xmla.catalog";
                String catalogName = systemsProps.getProperty(catalogProperty);
                
                if (catalogName == null) {
                    logger.warning("No catalog found for cube identifier: " + cubeIdentifier + ", using cube name as fallback");
                    catalogName = cubeName;
                }
                
                CubeConfig config = new CubeConfig(cubeName, catalogName);
                cubeConfigs.put(cubeIdentifier, config);
                logger.info("Found cube configuration - Identifier: " + cubeIdentifier + 
                           ", Cube: " + cubeName + 
                           ", Catalog: " + catalogName);
            }
        }
        
        if (cubeConfigs.isEmpty()) {
            logger.warning("No cube configurations found. Falling back to models.");
            // Fallback to models if no cube configurations found
            String models = systemsProps.getProperty("atscale.models");
            if (models != null && !models.trim().isEmpty()) {
                String[] modelList = models.split(",");
                for (String model : modelList) {
                    String trimmedModel = model.trim();
                    if (!trimmedModel.isEmpty()) {
                        cubeConfigs.put(trimmedModel, new CubeConfig(trimmedModel, trimmedModel));
                    }
                }
            }
        }
        
        return cubeConfigs;
    }
    
    private Map<String, String> loadConnectionDetails() {
        Map<String, String> connectionDetails = new HashMap<>();
        try {
            File connectionFile = new File("config.json");
            if (!connectionFile.exists()) {
                logger.severe("config.json file not found at: " + connectionFile.getAbsolutePath());
                return connectionDetails;
            }
            
            // Read JSON file without Jackson
            String jsonContent = new String(Files.readAllBytes(Paths.get("config.json")), StandardCharsets.UTF_8);
            // Simple JSON parsing for basic structure
            jsonContent = jsonContent.replace("{", "").replace("}", "").replace("\"", "").replaceAll("\\s+", "");
            String[] pairs = jsonContent.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":");
                if (keyValue.length == 2) {
                    connectionDetails.put(keyValue[0].trim(), keyValue[1].trim());
                }
            }
            
            logger.info("Loaded connection details for host: " + connectionDetails.get("host"));
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error loading config.json", e);
        }
        return connectionDetails;
    }
    
    private Properties loadSystemsProperties() {
        Properties props = new Properties();
        try (FileInputStream input = new FileInputStream(new File(WORKING_DIR, CONFIG_DIR + File.separator + "systems.properties"))) {
            props.load(new InputStreamReader(input, StandardCharsets.UTF_8));
            logger.info("Loaded systems.properties");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error loading systems.properties", e);
        }
        return props;
    }
    
    private List<MavenTaskDto<ClosedStep>> createTasksForCube(String cubeIdentifier, String cubeName, String catalogName, 
                                                             Map<String, String> connectionDetails, Properties systemsProps) {
        List<MavenTaskDto<ClosedStep>> cubeTasks = new ArrayList<>();
        
        try {
            // Check for JDBC CSV file
            String jdbcCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileName");
            boolean jdbcHasHeader = Boolean.parseBoolean(
                systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileHasHeader", "true")
            );
            
            if (jdbcCsvFile != null && !jdbcCsvFile.trim().isEmpty()) {
                File jdbcFile = new File(WORKING_DIR, INGEST_DIR + File.separator + jdbcCsvFile);
                if (jdbcFile.exists()) {
                    MavenTaskDto<ClosedStep> jdbcTask = createTaskForCsvFile(cubeName, catalogName, jdbcFile, 
                                                                            "jdbc", connectionDetails, jdbcHasHeader);
                    if (jdbcTask != null) {
                        cubeTasks.add(jdbcTask);
                        logger.info("Using CSV file for JDBC queries: " + jdbcCsvFile + " (hasHeader: " + jdbcHasHeader + ")");
                    } else {
                        logger.warning("Failed to create JDBC CSV task for cube: " + cubeName);
                    }
                } else {
                    logger.warning("JDBC CSV file not found: " + jdbcFile.getAbsolutePath());
                }
            } else {
                // Fall back to JSON if CSV not configured
                String jdbcFileName = cubeName + "_jdbc_queries.json";
                File jdbcFile = getQueryFileWithTimestamp(jdbcFileName);
                
                if (jdbcFile != null && jdbcFile.exists()) {
                    MavenTaskDto<ClosedStep> jdbcTask = createTaskForJsonFile(cubeName, catalogName, jdbcFile, 
                                                                             "jdbc", connectionDetails);
                    if (jdbcTask != null) {
                        cubeTasks.add(jdbcTask);
                    } else {
                        logger.warning("Failed to create JDBC JSON task for cube: " + cubeName);
                    }
                } else {
                    logger.warning("JDBC queries file not found for cube: " + cubeName + " (looking for: " + jdbcFileName + ")");
                }
            }
            
            // Check for XMLA CSV file
            String xmlaCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileName");
            boolean xmlaHasHeader = Boolean.parseBoolean(
                systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileHasHeader", "true")
            );
            
            if (xmlaCsvFile != null && !xmlaCsvFile.trim().isEmpty()) {
                File xmlaFile = new File(WORKING_DIR, INGEST_DIR + File.separator + xmlaCsvFile);
                if (xmlaFile.exists()) {
                    MavenTaskDto<ClosedStep> xmlaTask = createTaskForCsvFile(cubeName, catalogName, xmlaFile, 
                                                                            "xmla", connectionDetails, xmlaHasHeader);
                    if (xmlaTask != null) {
                        cubeTasks.add(xmlaTask);
                        logger.info("Using CSV file for XMLA queries: " + xmlaCsvFile + " (hasHeader: " + xmlaHasHeader + ")");
                    } else {
                        logger.warning("Failed to create XMLA CSV task for cube: " + cubeName);
                    }
                } else {
                    logger.warning("XMLA CSV file not found: " + xmlaFile.getAbsolutePath());
                }
            } else {
                // Fall back to JSON if CSV not configured
                String xmlaFileName = cubeName + "_xmla_queries.json";
                File xmlaFile = getQueryFileWithTimestamp(xmlaFileName);
                
                if (xmlaFile != null && xmlaFile.exists()) {
                    MavenTaskDto<ClosedStep> xmlaTask = createTaskForJsonFile(cubeName, catalogName, xmlaFile, 
                                                                             "xmla", connectionDetails);
                    if (xmlaTask != null) {
                        cubeTasks.add(xmlaTask);
                    } else {
                        logger.warning("Failed to create XMLA JSON task for cube: " + cubeName);
                    }
                } else {
                    logger.warning("XMLA queries file not found for cube: " + cubeName + " (looking for: " + xmlaFileName + ")");
                }
            }
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating tasks for cube: " + cubeName, e);
        }
        
        return cubeTasks;
    }
    
    private File getQueryFileWithTimestamp(String baseFileName) {
        try {
            File queriesDir = new File(WORKING_DIR, QUERIES_DIR);
            if (!queriesDir.exists()) {
                logger.warning("Queries directory not found: " + queriesDir.getAbsolutePath());
                return null;
            }
            
            // Look for files that match the base pattern (with or without timestamp)
            String baseNameWithoutExt = baseFileName.replace(".json", "");
            File[] matchingFiles = queriesDir.listFiles((dir, name) -> 
                name.startsWith(baseNameWithoutExt) && name.endsWith(".json")
            );
            
            if (matchingFiles == null || matchingFiles.length == 0) {
                return null;
            }
            
            // Return the most recently modified file
            Arrays.sort(matchingFiles, (f1, f2) -> 
                Long.compare(f2.lastModified(), f1.lastModified())
            );
            
            logger.fine("Found query file: " + matchingFiles[0].getName() + " for base: " + baseFileName);
            return matchingFiles[0];
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error finding query file: " + baseFileName, e);
            return null;
        }
    }
    
    // Create task for JSON file
    private MavenTaskDto<ClosedStep> createTaskForJsonFile(String cubeName, String catalogName, File queryFile, 
                                                          String protocol, Map<String, String> connectionDetails) {
        try {
            // Create task with descriptive name
            String taskName = String.format("%s %s Simulation (JSON)", cubeName, protocol.toUpperCase());
            MavenTaskDto<ClosedStep> task = new MavenTaskDto<>(taskName);

            // Set the simulation class based on protocol
            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);
            
            // Set the cube name as the model
            task.setModel(cubeName);
            
            // Set run description
            task.setRunDescription(String.format("%s %s Cube Tests (JSON)", cubeName, protocol.toUpperCase()));
            
            // Create injection steps
            List<ClosedStep> closedSteps = new ArrayList<>();
            closedSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 30)); // users, durationInSeconds
            
            // Set injection steps
            task.setInjectionSteps(closedSteps);
            
            // Set other required properties
            task.setMavenCommand("gatling:test");
            task.setRunId("SequentialRun-" + cubeName.replace(" ", "") + "-" + protocol + "-JSON");
            task.setRunLogFileName(cubeName.replace(" ", "_") + "_" + protocol + "_sequential.log");
            task.setLoggingAsAppend(false);
            
            // Build additional properties
            Map<String, String> additionalProperties = new HashMap<>();
            additionalProperties.put("queryFile", QUERIES_DIR + "/" + queryFile.getName());
            additionalProperties.put("queryFileType", "json");
            additionalProperties.put("modelName", cubeName);
            additionalProperties.put("catalogName", catalogName);
            additionalProperties.put("protocol", protocol);
            additionalProperties.put("baseUrl", "http://" + connectionDetails.get("host") + ":10500");
            additionalProperties.put("username", connectionDetails.get("username"));
            additionalProperties.put("password", connectionDetails.get("password"));
            additionalProperties.put("catalog", catalogName);
            
            // Add SSL properties - CRITICAL FIX for XMLA NullPointerException
            additionalProperties.put("javax.net.ssl.trustStore", "./cacerts");
            additionalProperties.put("javax.net.ssl.trustStorePassword", "changeit");
            additionalProperties.put("javax.net.ssl.keyStore", "./cacerts");
            additionalProperties.put("javax.net.ssl.keyStorePassword", "changeit");
            additionalProperties.put("sun.security.ssl.allowUnsafeRenegotiation", "true");
            
            // Use setAdditionalProperties with the Map
            task.setAdditionalProperties(additionalProperties);
            
            logger.info("Created sequential closed step task for cube: " + cubeName + 
                       " (catalog: " + catalogName + ")" +
                       ", protocol: " + protocol + 
                       ", file: " + queryFile.getName());
            
            return task;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }
    
    // Create task for CSV file
    private MavenTaskDto<ClosedStep> createTaskForCsvFile(String cubeName, String catalogName, File csvFile, 
                                                         String protocol, Map<String, String> connectionDetails, 
                                                         boolean hasHeader) {
        try {
            // Create task with descriptive name
            String taskName = String.format("%s %s Simulation (CSV)", cubeName, protocol.toUpperCase());
            MavenTaskDto<ClosedStep> task = new MavenTaskDto<>(taskName);

            // Set the simulation class based on protocol
            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);
            
            // Set the cube name as the model
            task.setModel(cubeName);
            
            // Set run description
            task.setRunDescription(String.format("%s %s Cube Tests (CSV)", cubeName, protocol.toUpperCase()));
            
            // Create injection steps
            List<ClosedStep> closedSteps = new ArrayList<>();
            closedSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 30)); // users, durationInSeconds
            
            // Set injection steps
            task.setInjectionSteps(closedSteps);
            
            // Set other required properties
            task.setMavenCommand("gatling:test");
            task.setRunId("SequentialRun-" + cubeName.replace(" ", "") + "-" + protocol + "-CSV");
            task.setRunLogFileName(cubeName.replace(" ", "_") + "_" + protocol + "_sequential_csv.log");
            task.setLoggingAsAppend(false);
            
            // Build additional properties
            Map<String, String> additionalProperties = new HashMap<>();
            additionalProperties.put("queryFile", INGEST_DIR + "/" + csvFile.getName());
            additionalProperties.put("queryFileType", "csv");
            additionalProperties.put("csvHasHeader", String.valueOf(hasHeader));
            additionalProperties.put("modelName", cubeName);
            additionalProperties.put("catalogName", catalogName);
            additionalProperties.put("protocol", protocol);
            additionalProperties.put("baseUrl", "http://" + connectionDetails.get("host") + ":10500");
            additionalProperties.put("username", connectionDetails.get("username"));
            additionalProperties.put("password", connectionDetails.get("password"));
            additionalProperties.put("catalog", catalogName);
            
            // Add SSL properties - CRITICAL FIX for XMLA NullPointerException
            additionalProperties.put("javax.net.ssl.trustStore", "./cacerts");
            additionalProperties.put("javax.net.ssl.trustStorePassword", "changeit");
            additionalProperties.put("javax.net.ssl.keyStore", "./cacerts");
            additionalProperties.put("javax.net.ssl.keyStorePassword", "changeit");
            additionalProperties.put("sun.security.ssl.allowUnsafeRenegotiation", "true");
            
            // Use setAdditionalProperties with the Map
            task.setAdditionalProperties(additionalProperties);
            
            logger.info("Created sequential closed step CSV task for cube: " + cubeName + 
                       " (catalog: " + catalogName + ")" +
                       ", protocol: " + protocol + 
                       ", file: " + csvFile.getName() +
                       ", hasHeader: " + hasHeader);
            
            return task;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating CSV task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }
    
    // Helper class to store cube configuration
    private static class CubeConfig {
        final String cubeName;
        final String catalogName;

        CubeConfig(String cubeName, String catalogName) {
            this.cubeName = cubeName;
            this.catalogName = catalogName;
        }
    }
}