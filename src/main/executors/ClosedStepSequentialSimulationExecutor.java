package executors;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.executors.SequentialSimulationExecutor;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.ConstantConcurrentUsersClosedInjectionStep;

public class ClosedStepSequentialSimulationExecutor extends SequentialSimulationExecutor<ClosedStep> {
    
    private static final Logger logger = Logger.getLogger(ClosedStepSequentialSimulationExecutor.class.getName());
    private static final String WORKING_DIR = "working_dir";
    private static final String QUERIES_DIR = "queries";
    private static final String INGEST_DIR = "ingest";
    private static final String CONFIG_DIR = "config";
    
    static {
        // Configure simple logging
        System.setProperty("java.util.logging.SimpleFormatter.format", 
                          "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }
    
    public static void main(String[] args) {
        logger.info("ClosedStepSequentialSimulationExecutor started.");
        
        ClosedStepSequentialSimulationExecutor executor = new ClosedStepSequentialSimulationExecutor();
        executor.execute();
        
        logger.info("ClosedStepSequentialSimulationExecutor completed.");
    }
    
    @Override
    protected List<MavenTaskDto<ClosedStep>> getSimulationTasks() {
        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();
        
        try {
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
                
                tasks.addAll(createTasksForCube(cubeIdentifier, config.cubeName, config.catalogName, systemsProps));
            }
            
            logger.info("Created " + tasks.size() + " closed step tasks for " + cubeConfigs.size() + " cubes");
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error loading tasks", e);
        }
        
        return tasks;
    }
    
    private Map<String, CubeConfig> getCubeConfigurations(Properties systemsProps) {
        Map<String, CubeConfig> cubeConfigs = new HashMap<>();
        
        for (String propertyName : systemsProps.stringPropertyNames()) {
            if (propertyName.startsWith("atscale.") && propertyName.contains(".xmla.cube")) {
                String cubeIdentifier = propertyName.substring("atscale.".length(), propertyName.indexOf(".xmla.cube"));
                String cubeName = systemsProps.getProperty(propertyName);
                
                String catalogProperty = "atscale." + cubeIdentifier + ".xmla.catalog";
                String catalogName = systemsProps.getProperty(catalogProperty);
                
                if (catalogName == null) {
                    logger.warning("No catalog found for cube identifier: " + cubeIdentifier + ", using cube name as fallback");
                    catalogName = cubeName;
                }
                
                CubeConfig config = new CubeConfig(cubeName, catalogName, cubeIdentifier);
                cubeConfigs.put(cubeIdentifier, config);
                logger.info("Found cube configuration - Identifier: " + cubeIdentifier + 
                           ", Cube: " + cubeName + 
                           ", Catalog: " + catalogName);
            }
        }
        
        if (cubeConfigs.isEmpty()) {
            logger.warning("No cube configurations found. Falling back to models.");
            String models = systemsProps.getProperty("atscale.models");
            if (models != null && !models.trim().isEmpty()) {
                String[] modelList = models.split(",");
                for (String model : modelList) {
                    String trimmedModel = model.trim();
                    if (!trimmedModel.isEmpty()) {
                        String sanitizedIdentifier = trimmedModel.replace(" ", "_");
                        cubeConfigs.put(sanitizedIdentifier, new CubeConfig(trimmedModel, trimmedModel, sanitizedIdentifier));
                    }
                }
            }
        }
        
        return cubeConfigs;
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
                                                             Properties systemsProps) {
        List<MavenTaskDto<ClosedStep>> cubeTasks = new ArrayList<>();
        
        try {
            // JDBC connection details
            Map<String, String> jdbcConnectionDetails = new HashMap<>();
            String jdbcUrl = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.url");
            if (jdbcUrl != null) {
                jdbcConnectionDetails.put("url", jdbcUrl);
                jdbcConnectionDetails.put("username", systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.username", ""));
                jdbcConnectionDetails.put("password", systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.password", ""));
            }
            
            // XMLA connection details
            Map<String, String> xmlaConnectionDetails = new HashMap<>();
            String xmlaUrl = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.url");
            if (xmlaUrl != null) {
                xmlaConnectionDetails.put("url", xmlaUrl);
                if (xmlaUrl.contains("/default/") && xmlaUrl.length() > xmlaUrl.indexOf("/default/") + 9) {
                    String afterDefault = xmlaUrl.substring(xmlaUrl.indexOf("/default/") + 9);
                    if (afterDefault.matches("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}.*")) {
                        xmlaConnectionDetails.put("authType", "token");
                    } else {
                        xmlaConnectionDetails.put("authType", "basic");
                        String authUrl = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.url");
                        if (authUrl != null) xmlaConnectionDetails.put("authUrl", authUrl);
                        String authUser = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.username");
                        String authPass = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.password");
                        if (authUser != null) xmlaConnectionDetails.put("username", authUser);
                        if (authPass != null) xmlaConnectionDetails.put("password", authPass);
                    }
                } else {
                    xmlaConnectionDetails.put("authType", "basic");
                    String authUrl = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.url");
                    if (authUrl != null) xmlaConnectionDetails.put("authUrl", authUrl);
                    String authUser = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.username");
                    String authPass = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.password");
                    if (authUser != null) xmlaConnectionDetails.put("username", authUser);
                    if (authPass != null) xmlaConnectionDetails.put("password", authPass);
                }
            }
            
            // JDBC tasks (JSON or CSV)
            String jdbcCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileName");
            boolean jdbcHasHeader = Boolean.parseBoolean(
                systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileHasHeader", "true")
            );
            
            if (jdbcCsvFile != null && !jdbcCsvFile.trim().isEmpty()) {
                File jdbcFile = new File(WORKING_DIR, INGEST_DIR + File.separator + jdbcCsvFile);
                if (jdbcFile.exists()) {
                    MavenTaskDto<ClosedStep> jdbcTask = createTaskForCsvFile(cubeName, catalogName, jdbcFile, 
                                                                            "jdbc", jdbcConnectionDetails, jdbcHasHeader, cubeIdentifier);
                    if (jdbcTask != null) cubeTasks.add(jdbcTask);
                } else {
                    logger.warning("JDBC CSV file not found: " + jdbcFile.getAbsolutePath());
                }
            } else {
                String jdbcFileName = cubeName + "_jdbc_queries.json";
                File jdbcFile = getQueryFile(jdbcFileName);
                
                if (jdbcFile != null && jdbcFile.exists()) {
                    MavenTaskDto<ClosedStep> jdbcTask = createTaskForJsonFile(cubeName, catalogName, jdbcFile, 
                                                                             "jdbc", jdbcConnectionDetails, cubeIdentifier);
                    if (jdbcTask != null) cubeTasks.add(jdbcTask);
                } else {
                    logger.fine("JDBC queries file not found for cube: " + cubeName + " (looking for: " + jdbcFileName + ")");
                }
            }
            
            // XMLA tasks (JSON or CSV)
            String xmlaCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileName");
            boolean xmlaHasHeader = Boolean.parseBoolean(
                systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileHasHeader", "true")
            );
            
            if (xmlaCsvFile != null && !xmlaCsvFile.trim().isEmpty()) {
                File xmlaFile = new File(WORKING_DIR, INGEST_DIR + File.separator + xmlaCsvFile);
                if (xmlaFile.exists()) {
                    MavenTaskDto<ClosedStep> xmlaTask = createTaskForCsvFile(cubeName, catalogName, xmlaFile, 
                                                                            "xmla", xmlaConnectionDetails, xmlaHasHeader, cubeIdentifier);
                    if (xmlaTask != null) cubeTasks.add(xmlaTask);
                } else {
                    logger.warning("XMLA CSV file not found: " + xmlaFile.getAbsolutePath());
                }
            } else {
                String xmlaFileName = cubeName + "_xmla_queries.json";
                File xmlaFile = getQueryFile(xmlaFileName);
                
                if (xmlaFile != null && xmlaFile.exists()) {
                    MavenTaskDto<ClosedStep> xmlaTask = createTaskForJsonFile(cubeName, catalogName, xmlaFile, 
                                                                             "xmla", xmlaConnectionDetails, cubeIdentifier);
                    if (xmlaTask != null) cubeTasks.add(xmlaTask);
                } else {
                    logger.fine("XMLA queries file not found for cube: " + cubeName + " (looking for: " + xmlaFileName + ")");
                }
            }
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating tasks for cube: " + cubeName, e);
        }
        
        return cubeTasks;
    }
    
    private File getQueryFile(String fileName) {
        try {
            File queriesDir = new File(WORKING_DIR, QUERIES_DIR);
            if (!queriesDir.exists()) {
                logger.warning("Queries directory not found: " + queriesDir.getAbsolutePath());
                return null;
            }
            
            File exactFile = new File(queriesDir, fileName);
            if (exactFile.exists()) {
                return exactFile;
            }
            
            String baseNameWithoutExt = fileName.replace(".json", "");
            File[] matchingFiles = queriesDir.listFiles((dir, name) -> 
                name.startsWith(baseNameWithoutExt + "_") && name.endsWith(".json")
            );
            
            if (matchingFiles == null || matchingFiles.length == 0) {
                return null;
            }
            
            Arrays.sort(matchingFiles, (f1, f2) -> 
                Long.compare(f2.lastModified(), f1.lastModified())
            );
            
            return matchingFiles[0];
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error finding query file: " + fileName, e);
            return null;
        }
    }
    
    private MavenTaskDto<ClosedStep> createTaskForJsonFile(String cubeName, String catalogName, File queryFile, 
                                                          String protocol, Map<String, String> connectionDetails,
                                                          String cubeIdentifier) {
        try {
            String queryFilePath = QUERIES_DIR + "/" + queryFile.getName();
            String sanitizedCubeNameForLogs = cubeName.replace(" ", "_");
            String sanitizedCatalogNameForLogs = catalogName.replace(" ", "_");
            
            String taskName = String.format("%s %s Closed Simulation (JSON)", cubeName, protocol.toUpperCase());
            MavenTaskDto<ClosedStep> task = new MavenTaskDto<>(taskName);

            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);
            
            // Use exact cube name as model
            task.setModel(cubeName);
            
            task.setRunDescription(String.format("%s %s Closed Cube Tests (JSON)", cubeName, protocol.toUpperCase()));
            
            List<ClosedStep> closedSteps = new ArrayList<>();
            // match what the concurrent used:
            closedSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 30)); // users, durationInSeconds
            task.setInjectionSteps(closedSteps);
            
            task.setMavenCommand("gatling:test");
            task.setRunId("ClosedRun-" + sanitizedCatalogNameForLogs + "-" + sanitizedCubeNameForLogs + "-" + protocol + "-JSON");
            task.setRunLogFileName(sanitizedCatalogNameForLogs + "_" + sanitizedCubeNameForLogs + "_" + protocol + "_closed.log");
            task.setLoggingAsAppend(false);
            
            Map<String, String> additionalProperties = new HashMap<>();
            // Pass the full query file path relative to working_dir
            additionalProperties.put("queryFile", queryFilePath);
            additionalProperties.put("queryFileType", "json");
            additionalProperties.put("catalogName", catalogName);
            additionalProperties.put("cubeName", cubeName);
            additionalProperties.put("cubeIdentifier", cubeIdentifier);
            
            additionalProperties.put("url", connectionDetails.getOrDefault("url", ""));
            additionalProperties.put("username", connectionDetails.getOrDefault("username", ""));
            additionalProperties.put("password", connectionDetails.getOrDefault("password", ""));
            additionalProperties.put("protocol", protocol);
            
            if ("xmla".equals(protocol)) {
                additionalProperties.put("xmlaUrl", connectionDetails.getOrDefault("url", ""));
                additionalProperties.put("authType", connectionDetails.getOrDefault("authType", "basic"));
                
                if (!"token".equals(connectionDetails.get("authType"))) {
                    additionalProperties.put("authUrl", connectionDetails.getOrDefault("authUrl", ""));
                }
                
                additionalProperties.put("catalog", catalogName);
            }
            
            // SSL/truststore props
            String trustStore = "./cacerts";
            additionalProperties.put("javax.net.ssl.trustStore", trustStore);
            additionalProperties.put("javax.net.ssl.trustStorePassword", "changeit");
            
            task.setAdditionalProperties(additionalProperties);
            
            logger.info("Created closed step task - Cube: " + cubeName + 
                       ", Catalog: " + catalogName + 
                       ", Identifier: " + cubeIdentifier +
                       ", Protocol: " + protocol + 
                       ", Query file: " + queryFile.getName() +
                       ", Model set to: " + cubeName);
            
            return task;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }
    
    private MavenTaskDto<ClosedStep> createTaskForCsvFile(String cubeName, String catalogName, File csvFile, 
                                                         String protocol, Map<String, String> connectionDetails, 
                                                         boolean hasHeader, String cubeIdentifier) {
        try {
            String sanitizedCubeNameForLogs = cubeName.replace(" ", "_");
            String sanitizedCatalogNameForLogs = catalogName.replace(" ", "_");
            
            String taskName = String.format("%s %s Closed Simulation (CSV)", cubeName, protocol.toUpperCase());
            MavenTaskDto<ClosedStep> task = new MavenTaskDto<>(taskName);

            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);
            
            // Use exact cube name as model
            task.setModel(cubeName);
            
            task.setRunDescription(String.format("%s %s Closed Cube Tests (CSV)", cubeName, protocol.toUpperCase()));
            
            List<ClosedStep> closedSteps = new ArrayList<>();
            closedSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 30)); // users, durationInSeconds
            task.setInjectionSteps(closedSteps);
            
            task.setMavenCommand("gatling:test");
            task.setRunId("ClosedRun-" + sanitizedCatalogNameForLogs + "-" + sanitizedCubeNameForLogs + "-" + protocol + "-CSV");
            task.setRunLogFileName(sanitizedCatalogNameForLogs + "_" + sanitizedCubeNameForLogs + "_" + protocol + "_closed_csv.log");
            task.setLoggingAsAppend(false);
            
            Map<String, String> additionalProperties = new HashMap<>();
            additionalProperties.put("queryFile", INGEST_DIR + "/" + csvFile.getName());
            additionalProperties.put("queryFileType", "csv");
            additionalProperties.put("csvHasHeader", String.valueOf(hasHeader));
            additionalProperties.put("catalogName", catalogName);
            additionalProperties.put("cubeName", cubeName);
            additionalProperties.put("cubeIdentifier", cubeIdentifier);
            
            additionalProperties.put("url", connectionDetails.getOrDefault("url", ""));
            additionalProperties.put("username", connectionDetails.getOrDefault("username", ""));
            additionalProperties.put("password", connectionDetails.getOrDefault("password", ""));
            additionalProperties.put("protocol", protocol);
            
            if ("xmla".equals(protocol)) {
                additionalProperties.put("xmlaUrl", connectionDetails.getOrDefault("url", ""));
                additionalProperties.put("authType", connectionDetails.getOrDefault("authType", "basic"));
                
                if (!"token".equals(connectionDetails.get("authType"))) {
                    additionalProperties.put("authUrl", connectionDetails.getOrDefault("authUrl", ""));
                }
                
                additionalProperties.put("catalog", catalogName);
            }
            
            // SSL/truststore props
            String trustStore = "./cacerts";
            additionalProperties.put("javax.net.ssl.trustStore", trustStore);
            additionalProperties.put("javax.net.ssl.trustStorePassword", "changeit");
            
            task.setAdditionalProperties(additionalProperties);
            
            logger.info("Created closed step CSV task - Cube: " + cubeName + 
                       ", Catalog: " + catalogName + 
                       ", Identifier: " + cubeIdentifier +
                       ", Protocol: " + protocol + 
                       ", CSV file: " + csvFile.getName() +
                       ", Has header: " + hasHeader +
                       ", Model set to: " + cubeName);
            
            return task;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating CSV task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }
    
    private static class CubeConfig {
        final String cubeName;
        final String catalogName;
        final String identifier;

        CubeConfig(String cubeName, String catalogName, String identifier) {
            this.cubeName = cubeName;
            this.catalogName = catalogName;
            this.identifier = identifier;
        }
    }
}