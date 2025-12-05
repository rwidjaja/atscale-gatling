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
import com.atscale.java.injectionsteps.AtOnceUsersOpenInjectionStep;
import com.atscale.java.injectionsteps.OpenStep;

public class OpenStepSequentialSimulationExecutor extends SequentialSimulationExecutor<OpenStep> {
    
    private static final Logger logger = Logger.getLogger(OpenStepSequentialSimulationExecutor.class.getName());
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
        logger.info("OpenStepSequentialSimulationExecutor started.");
        
        OpenStepSequentialSimulationExecutor executor = new OpenStepSequentialSimulationExecutor();
        executor.execute();
        
        logger.info("OpenStepSequentialSimulationExecutor completed.");
    }
    
    @Override
    protected List<MavenTaskDto<OpenStep>> getSimulationTasks() {
        List<MavenTaskDto<OpenStep>> tasks = new ArrayList<>();
        
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
            
            logger.info("Created " + tasks.size() + " open step tasks for " + cubeConfigs.size() + " cubes");
            
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
    
    private List<MavenTaskDto<OpenStep>> createTasksForCube(String cubeIdentifier, String cubeName, String catalogName, 
                                                           Properties systemsProps) {
        List<MavenTaskDto<OpenStep>> cubeTasks = new ArrayList<>();
        
        try {
            Map<String, String> jdbcConnectionDetails = getJdbcConnectionDetails(cubeIdentifier, systemsProps);
            Map<String, String> xmlaConnectionDetails = getXmlaConnectionDetails(cubeIdentifier, systemsProps);
            
            String jdbcCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileName");
            boolean jdbcHasHeader = Boolean.parseBoolean(
                systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileHasHeader", "true")
            );
            
            if (jdbcCsvFile != null && !jdbcCsvFile.trim().isEmpty()) {
                File jdbcFile = new File(WORKING_DIR, INGEST_DIR + File.separator + jdbcCsvFile);
                if (jdbcFile.exists()) {
                    MavenTaskDto<OpenStep> jdbcTask = createTaskForCsvFile(cubeName, catalogName, jdbcFile, 
                                                                          "jdbc", jdbcConnectionDetails, jdbcHasHeader, cubeIdentifier);
                    if (jdbcTask != null) {
                        cubeTasks.add(jdbcTask);
                    }
                } else {
                    logger.warning("JDBC CSV file not found: " + jdbcFile.getAbsolutePath());
                }
            } else {
                String jdbcFileName = cubeName + "_jdbc_queries.json";
                File jdbcFile = getQueryFile(jdbcFileName);
                
                if (jdbcFile != null && jdbcFile.exists()) {
                    MavenTaskDto<OpenStep> jdbcTask = createTaskForJsonFile(cubeName, catalogName, jdbcFile, 
                                                                           "jdbc", jdbcConnectionDetails, cubeIdentifier);
                    if (jdbcTask != null) {
                        cubeTasks.add(jdbcTask);
                    }
                } else {
                    logger.warning("JDBC queries file not found for cube: " + cubeName + 
                                 " (looking for: " + jdbcFileName + ")");
                }
            }
            
            String xmlaCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileName");
            boolean xmlaHasHeader = Boolean.parseBoolean(
                systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileHasHeader", "true")
            );
            
            if (xmlaCsvFile != null && !xmlaCsvFile.trim().isEmpty()) {
                File xmlaFile = new File(WORKING_DIR, INGEST_DIR + File.separator + xmlaCsvFile);
                if (xmlaFile.exists()) {
                    MavenTaskDto<OpenStep> xmlaTask = createTaskForCsvFile(cubeName, catalogName, xmlaFile, 
                                                                          "xmla", xmlaConnectionDetails, xmlaHasHeader, cubeIdentifier);
                    if (xmlaTask != null) {
                        cubeTasks.add(xmlaTask);
                    }
                } else {
                    logger.warning("XMLA CSV file not found: " + xmlaFile.getAbsolutePath());
                }
            } else {
                String xmlaFileName = cubeName + "_xmla_queries.json";
                File xmlaFile = getQueryFile(xmlaFileName);
                
                if (xmlaFile != null && xmlaFile.exists()) {
                    MavenTaskDto<OpenStep> xmlaTask = createTaskForJsonFile(cubeName, catalogName, xmlaFile, 
                                                                           "xmla", xmlaConnectionDetails, cubeIdentifier);
                    if (xmlaTask != null) {
                        cubeTasks.add(xmlaTask);
                    }
                } else {
                    logger.warning("XMLA queries file not found for cube: " + cubeName + 
                                 " (looking for: " + xmlaFileName + ")");
                }
            }
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating tasks for cube: " + cubeName, e);
        }
        
        return cubeTasks;
    }
    
    private Map<String, String> getJdbcConnectionDetails(String cubeIdentifier, Properties systemsProps) {
        Map<String, String> connectionDetails = new HashMap<>();
        
        String url = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.url");
        String username = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.username");
        String password = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.password");
        
        if (url != null) {
            connectionDetails.put("url", url);
            connectionDetails.put("username", username != null ? username : "");
            connectionDetails.put("password", password != null ? password : "");
            logger.info("Found JDBC connection details for cube identifier: " + cubeIdentifier);
        } else {
            logger.warning("No JDBC URL found for cube identifier: " + cubeIdentifier);
        }
        
        return connectionDetails;
    }
    
    private Map<String, String> getXmlaConnectionDetails(String cubeIdentifier, Properties systemsProps) {
        Map<String, String> connectionDetails = new HashMap<>();
        
        String url = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.url");
        
        if (url != null) {
            connectionDetails.put("url", url);
            
            if (url.contains("/default/") && url.length() > url.indexOf("/default/") + 9) {
                String afterDefault = url.substring(url.indexOf("/default/") + 9);
                if (afterDefault.matches("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}.*")) {
                    logger.info("XMLA URL contains token for cube identifier: " + cubeIdentifier + " - using token-based auth");
                    connectionDetails.put("authType", "token");
                } else {
                    connectionDetails.put("authType", "basic");
                    String authUrl = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.url");
                    String username = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.username");
                    String password = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.password");
                    
                    if (authUrl != null) connectionDetails.put("authUrl", authUrl);
                    if (username != null) connectionDetails.put("username", username);
                    if (password != null) connectionDetails.put("password", password);
                }
            } else {
                connectionDetails.put("authType", "basic");
                String authUrl = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.url");
                String username = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.username");
                String password = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.auth.password");
                
                if (authUrl != null) connectionDetails.put("authUrl", authUrl);
                if (username != null) connectionDetails.put("username", username);
                if (password != null) connectionDetails.put("password", password);
            }
            
            logger.info("Found XMLA connection details for cube identifier: " + cubeIdentifier + 
                       ", auth type: " + connectionDetails.get("authType"));
        } else {
            logger.warning("No XMLA URL found for cube identifier: " + cubeIdentifier);
        }
        
        return connectionDetails;
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
                logger.fine("Found exact query file: " + exactFile.getName());
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
            
            logger.fine("Found timestamped query file: " + matchingFiles[0].getName() + " for base: " + fileName);
            return matchingFiles[0];
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error finding query file: " + fileName, e);
            return null;
        }
    }
    
    private MavenTaskDto<OpenStep> createTaskForJsonFile(String cubeName, String catalogName, File queryFile, 
                                                        String protocol, Map<String, String> connectionDetails,
                                                        String cubeIdentifier) {
        try {
            String queryFilePath = QUERIES_DIR + "/" + queryFile.getName();
            
            // Sanitize only for log file names, NOT for cube name passed to simulation
            String sanitizedCubeNameForLogs = cubeName.replace(" ", "_");
            String sanitizedCatalogNameForLogs = catalogName.replace(" ", "_");
            
            String taskName = String.format("%s %s Open Simulation (JSON)", cubeName, protocol.toUpperCase());
            MavenTaskDto<OpenStep> task = new MavenTaskDto<>(taskName);

            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);
            
            // FIX: Use the EXACT cube name (with spaces) as the model for the simulation
            // This is what the simulation will use to construct the filename
            task.setModel(cubeName);
            
            task.setRunDescription(String.format("%s %s Open Cube Tests (JSON)", cubeName, protocol.toUpperCase()));
            
            List<OpenStep> openSteps = new ArrayList<>();
            openSteps.add(new AtOnceUsersOpenInjectionStep(1));
            
            task.setInjectionSteps(openSteps);
            
            task.setMavenCommand("gatling:test");
            task.setRunId("OpenRun-" + sanitizedCatalogNameForLogs + "-" + sanitizedCubeNameForLogs + "-" + protocol + "-JSON");
            task.setRunLogFileName(sanitizedCatalogNameForLogs + "_" + sanitizedCubeNameForLogs + "_" + protocol + "_open.log");
            task.setLoggingAsAppend(false);
            
            Map<String, String> additionalProperties = new HashMap<>();
            // Pass the full query file path
            additionalProperties.put("queryFile", queryFilePath);
            additionalProperties.put("queryFileType", "json");
            additionalProperties.put("catalogName", catalogName);
            // Pass the exact cube name (with spaces) for the simulation to use
            additionalProperties.put("cubeName", cubeName);
            // Also pass the identifier for property lookup if needed
            additionalProperties.put("cubeIdentifier", cubeIdentifier);
            
            // Pass connection details directly
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
            
            String trustStore = "./cacerts";
            additionalProperties.put("javax.net.ssl.trustStore", trustStore);
            additionalProperties.put("javax.net.ssl.trustStorePassword", "changeit");
            
            task.setAdditionalProperties(additionalProperties);
            
            logger.info("Created open step task - Cube: " + cubeName + 
                       ", Catalog: " + catalogName + 
                       ", Identifier: " + cubeIdentifier +
                       ", Protocol: " + protocol + 
                       ", Query file: " + queryFile.getName() +
                       ", Model set to: " + cubeName); // Log what we're setting as model
            
            return task;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }
    
    private MavenTaskDto<OpenStep> createTaskForCsvFile(String cubeName, String catalogName, File csvFile, 
                                                       String protocol, Map<String, String> connectionDetails, 
                                                       boolean hasHeader, String cubeIdentifier) {
        try {
            String sanitizedCubeNameForLogs = cubeName.replace(" ", "_");
            String sanitizedCatalogNameForLogs = catalogName.replace(" ", "_");
            
            String taskName = String.format("%s %s Open Simulation (CSV)", cubeName, protocol.toUpperCase());
            MavenTaskDto<OpenStep> task = new MavenTaskDto<>(taskName);

            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);
            
            // FIX: Use the EXACT cube name (with spaces) as the model for the simulation
            task.setModel(cubeName);
            
            task.setRunDescription(String.format("%s %s Open Cube Tests (CSV)", cubeName, protocol.toUpperCase()));
            
            List<OpenStep> openSteps = new ArrayList<>();
            openSteps.add(new AtOnceUsersOpenInjectionStep(1));
            
            task.setInjectionSteps(openSteps);
            
            task.setMavenCommand("gatling:test");
            task.setRunId("OpenRun-" + sanitizedCatalogNameForLogs + "-" + sanitizedCubeNameForLogs + "-" + protocol + "-CSV");
            task.setRunLogFileName(sanitizedCatalogNameForLogs + "_" + sanitizedCubeNameForLogs + "_" + protocol + "_open_csv.log");
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
            
            String trustStore = "./cacerts";
            additionalProperties.put("javax.net.ssl.trustStore", trustStore);
            additionalProperties.put("javax.net.ssl.trustStorePassword", "changeit");
            
            task.setAdditionalProperties(additionalProperties);
            
            logger.info("Created open step CSV task - Cube: " + cubeName + 
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