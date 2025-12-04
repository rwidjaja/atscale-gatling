package executors;

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

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.executors.SimulationExecutor;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.ConstantConcurrentUsersClosedInjectionStep;

public class ClosedStepSequentialSimulationExecutor extends SimulationExecutor {
    
    private static final Logger logger = Logger.getLogger(ClosedStepSequentialSimulationExecutor.class.getName());
    private static final String WORKING_DIR = "working_dir";
    private static final String QUERIES_DIR = "queries";
    private static final String CONFIG_DIR = "config";
    
    static {
        // Configure simple logging
        System.setProperty("java.util.logging.SimpleFormatter.format", 
                          "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }
    
    public static void main(String[] args) {
        logger.info("ClosedStepSequentialSimulationExecutor started.");
        
        ClosedStepSequentialSimulationExecutor executor = new ClosedStepSequentialSimulationExecutor();
        
        // Get the tasks - use the generic type
        List<MavenTaskDto<ClosedStep>> tasks = executor.getTasks();
        logger.info("Created " + tasks.size() + " tasks.");
        
        // Print task details for debugging
        for (MavenTaskDto<ClosedStep> task : tasks) {
            logger.info("Task: " + task.getTaskName() + " - Simulation: " + task.getSimulationClass());
        }
        
        // EXECUTE THE SIMULATIONS - This is what you're missing!
        logger.info("Starting simulation execution...");
        executor.executeTasks(tasks);
        
        logger.info("ClosedStepSequentialSimulationExecutor completed.");
    }
    
    // ADD THIS METHOD TO EXECUTE THE TASKS
    private void executeTasks(List<MavenTaskDto<ClosedStep>> tasks) {
        for (MavenTaskDto<ClosedStep> task : tasks) {
            try {
                logger.info("=== EXECUTING SIMULATION ===");
                logger.info("Task: " + task.getTaskName());
                logger.info("Simulation: " + task.getSimulationClass());
                logger.info("Model: " + task.getModel());
                
                // Decode the additional properties to see what we're sending
                String additionalPropsStr = task.getAdditionalProperties();
                Map<String, String> additionalProps = task.decodeAdditionalProperties(additionalPropsStr);
                
                if (additionalProps != null) {
                    logger.info("Protocol: " + additionalProps.get("protocol"));
                    logger.info("Query File: " + additionalProps.get("queryFile"));
                    logger.info("Target: " + additionalProps.get("baseUrl"));
                    logger.info("Catalog: " + additionalProps.get("catalog"));
                }
                
                // Execute the simulation using the base class method
                executeSimulation(task);
                
                logger.info("=== COMPLETED SIMULATION ===");
                
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to execute task: " + task.getTaskName(), e);
            }
        }
    }
    
    // ADD THIS METHOD TO HANDLE SIMULATION EXECUTION
private void executeSimulation(MavenTaskDto<ClosedStep> task) {
    try {
        // Build the Maven command with all required properties
        List<String> command = new ArrayList<>();
        command.add("mvn");
        command.add(task.getMavenCommand());
        command.add("-D" + MavenTaskDto.GATLING_SIMULATION_CLASS + "=" + task.getSimulationClass());
        
        // Add model and runId as system properties
        command.add("-D" + MavenTaskDto.ATSCALE_MODEL + "=" + task.getModel());
        command.add("-D" + MavenTaskDto.GATLING_RUN_ID + "=" + task.getRunId());
        command.add("-D" + MavenTaskDto.GATLING_RUN_DESCRIPTION + "=" + task.getRunDescription());
        
        // Add injection steps
        command.add("-D" + MavenTaskDto.GATLING_INJECTION_STEPS + "=" + task.getInjectionSteps());
        
        // Add logging properties
        command.add("-D" + MavenTaskDto.GATLING_RUN_LOGFILENAME + "=" + task.getRunLogFileName());
        command.add("-D" + MavenTaskDto.GATLING_RUN_LOGAPPEND + "=" + task.isRunLogAppend());
        
        // Add additional properties as a single encoded property
        command.add("-D" + MavenTaskDto.ADDITIONAL_PROPERTIES + "=" + task.getAdditionalProperties());
        
        // Add SSL properties to ensure the certificate is trusted
        String javaHome = System.getProperty("java.home");
        String trustStore = "./cacerts";
        command.add("-Djavax.net.ssl.trustStore=" + trustStore);
        command.add("-Djavax.net.ssl.trustStorePassword=changeit");
        
        logger.info("Executing command: " + String.join(" ", command));
        logger.info("Using truststore: " + trustStore);
        
        // Actually execute the command
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File(".")); // Set working directory to current project
        pb.redirectErrorStream(true); // Combine stdout and stderr
        
        // Add JAVA_HOME to environment to ensure same JVM is used
        Map<String, String> env = pb.environment();
        env.put("JAVA_HOME", javaHome);
        
        Process process = pb.start();
        
        // Read and log the output
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info("GATLING: " + line);
                
                // Look for query execution indicators in the output
                if (line.contains("REQUEST") || line.contains("query") || 
                    line.contains("SQL") || line.contains("XMLA") ||
                    line.contains("executing") || line.contains("submitting")) {
                    logger.info("QUERY ACTIVITY DETECTED: " + line);
                }
            }
        }
        
        int exitCode = process.waitFor();
        if (exitCode == 0) {
            logger.info("Simulation completed successfully: " + task.getTaskName());
        } else {
            logger.warning("Simulation exited with code " + exitCode + ": " + task.getTaskName());
        }
        
    } catch (Exception e) {
        logger.log(Level.SEVERE, "Error executing simulation: " + task.getTaskName(), e);
    }
}
    
    public List<MavenTaskDto<ClosedStep>> getTasks() {
        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        
        try {
            // Load connection details from JSON file
            Map<String, String> connectionDetails = loadConnectionDetails();
            if (connectionDetails.isEmpty()) {
                logger.severe("Failed to load connection details from config.json");
                return tasks;
            }
            
            // Load cube configurations from systems.properties
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
                
                tasks.addAll(createTasksForCube(config.cubeName, config.catalogName, connectionDetails, systemsProps));
            }
            
            logger.info("Created " + tasks.size() + " tasks for " + cubeConfigs.size() + " cubes");
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error loading tasks", e);
        }
        
        return tasks;
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
    
    private List<MavenTaskDto<ClosedStep>> createTasksForCube(String cubeName, String catalogName, Map<String, String> connectionDetails, Properties systemsProps) {
        List<MavenTaskDto<ClosedStep>> cubeTasks = new ArrayList<>();
        
        try {
            // Check for JDBC queries file - use the actual cube name for file lookup
            String jdbcFileName = cubeName + "_jdbc_queries.json";
            File jdbcFile = getQueryFileWithTimestamp(jdbcFileName);
            
            if (jdbcFile != null && jdbcFile.exists()) {
                MavenTaskDto<ClosedStep> jdbcTask = createTaskForQueryFile(cubeName, catalogName, jdbcFile, "jdbc", connectionDetails);
                if (jdbcTask != null) {
                    cubeTasks.add(jdbcTask);
                }
            } else {
                logger.warning("JDBC queries file not found for cube: " + cubeName + " (looking for: " + jdbcFileName + ")");
            }
            
            // Check for XMLA queries file - use the actual cube name for file lookup
            String xmlaFileName = cubeName + "_xmla_queries.json";
            File xmlaFile = getQueryFileWithTimestamp(xmlaFileName);
            
            if (xmlaFile != null && xmlaFile.exists()) {
                MavenTaskDto<ClosedStep> xmlaTask = createTaskForQueryFile(cubeName, catalogName, xmlaFile, "xmla", connectionDetails);
                if (xmlaTask != null) {
                    cubeTasks.add(xmlaTask);
                }
            } else {
                logger.warning("XMLA queries file not found for cube: " + cubeName + " (looking for: " + xmlaFileName + ")");
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
    
   private MavenTaskDto<ClosedStep> createTaskForQueryFile(String cubeName, String catalogName, File queryFile, String protocol, Map<String, String> connectionDetails) {
    try {
        // Create task with descriptive name
        String taskName = String.format("%s %s User Simulation", cubeName, protocol.toUpperCase());
        MavenTaskDto<ClosedStep> task = new MavenTaskDto<>(taskName);

        // Set the simulation class based on protocol
        String simulationClass;
        if ("xmla".equals(protocol)) {
            simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation";
        } else {
            simulationClass = "com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation";
        }
        task.setSimulationClass(simulationClass);
        
        // Set the cube name as the model (for both JDBC and XMLA) - this is what PropertiesManager will use
        task.setModel(cubeName);
        
        // Set run description
        task.setRunDescription(String.format("%s %s Cube Tests", cubeName, protocol.toUpperCase()));
        
        // Create injection steps using the correct constructor signature
        List<ClosedStep> closedSteps = new ArrayList<>();
        closedSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 30)); // users, durationInSeconds
        
        // Set injection steps
        task.setInjectionSteps(closedSteps);
        
        // Set other required properties from the original example
        task.setMavenCommand("gatling:test");
        task.setRunId("DynamicRun-" + cubeName.replace(" ", "") + "-" + protocol);
        task.setRunLogFileName(cubeName.replace(" ", "_") + "_" + protocol + ".log");
        task.setLoggingAsAppend(false);
        
        // Build additional properties as a Map (which will be JSON encoded and Base64 encoded by MavenTaskDto)
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("queryFile", QUERIES_DIR + "/" + queryFile.getName());
        additionalProperties.put("modelName", cubeName); // Use cube name for both JDBC and XMLA
        additionalProperties.put("catalogName", catalogName); // Add catalog name for reference
        additionalProperties.put("protocol", protocol);
        additionalProperties.put("baseUrl", "http://" + connectionDetails.get("host") + ":10500");
        additionalProperties.put("username", connectionDetails.get("username"));
        additionalProperties.put("password", connectionDetails.get("password"));
        additionalProperties.put("catalog", catalogName); // Add catalog explicitly for XMLA
        
        // Use setAdditionalProperties with the Map (MavenTaskDto will handle encoding)
        task.setAdditionalProperties(additionalProperties);
        
        logger.info("Created task for cube: " + cubeName + 
                   " (catalog: " + catalogName + ")" +
                   ", protocol: " + protocol + 
                   ", file: " + queryFile.getName());
        
        return task;
        
    } catch (Exception e) {
        logger.log(Level.SEVERE, "Error creating task for cube: " + cubeName + ", protocol: " + protocol, e);
        return null;
    }
}
    
    public String getExecutorName() {
        return "ClosedStepSequentialSimulationExecutor";
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