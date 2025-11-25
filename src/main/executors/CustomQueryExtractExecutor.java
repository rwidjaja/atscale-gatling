package executors;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.dao.AtScalePostgresDao;
import com.atscale.java.executors.SimulationExecutor;
import com.atscale.java.utils.QueryHistoryFileUtil;

public class CustomQueryExtractExecutor extends SimulationExecutor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomQueryExtractExecutor.class);
    private static final String WORKING_DIR = "working_dir";
    private static final String CONFIG_DIR = "config";
    
    public static void main(String[] args) {
        LOGGER.info("CustomQueryExtractExecutor started.");
        
        try {
            CustomQueryExtractExecutor executor = new CustomQueryExtractExecutor();
            executor.extractCustomQueries();
            LOGGER.info("CustomQueryExtractExecutor completed successfully.");
        } catch (Exception e) {
            LOGGER.error("CustomQueryExtractExecutor failed", e);
        } finally {
            // Explicitly shutdown Log4j2 to prevent the async shutdown error
            org.apache.logging.log4j.LogManager.shutdown();
        }
    }
    
    public void extractCustomQueries() {
        LOGGER.info("Step 1: Starting custom query extraction process");
        
        try {
            // Step 2: Load systems properties
            LOGGER.info("Step 2: Loading systems.properties from {}/{}", WORKING_DIR, CONFIG_DIR);
            Properties systemsProps = loadSystemsProperties();
            
            // Get cube configurations with actual cube names
            Map<String, String> cubeConfigs = getCubeConfigurations(systemsProps);
            
            if (cubeConfigs.isEmpty()) {
                LOGGER.error("No cube configurations found in systems.properties");
                return;
            }
            
            LOGGER.info("Processing {} cubes: {}", cubeConfigs.size(), cubeConfigs.values());
            
            // Step 3: Initialize query extractor
            LOGGER.info("Step 3: Initializing QueryHistoryFileUtil with AtScalePostgresDao");
            QueryHistoryFileUtil queryExtractor = new QueryHistoryFileUtil(AtScalePostgresDao.getInstance());
            LOGGER.info("QueryHistoryFileUtil initialized successfully");
            
            // Step 4: Process each cube
            LOGGER.info("Step 4: Starting to process {} cubes", cubeConfigs.size());
            int successCount = 0;
            int errorCount = 0;
            
            for (Map.Entry<String, String> cubeEntry : cubeConfigs.entrySet()) {
                String cubeIdentifier = cubeEntry.getKey();
                String actualCubeName = cubeEntry.getValue();
                
                LOGGER.info("Processing cube - Identifier: '{}', Actual Name: '{}'", cubeIdentifier, actualCubeName);
                boolean success = extractQueriesForCube(actualCubeName, queryExtractor, systemsProps);
                if (success) {
                    successCount++;
                    LOGGER.info("Successfully processed cube: {}", actualCubeName);
                } else {
                    errorCount++;
                    LOGGER.warn("Failed to process cube: {}", actualCubeName);
                }
            }
            
            LOGGER.info("Extraction Summary: {} successful, {} failed out of {} total cubes", 
                       successCount, errorCount, cubeConfigs.size());
            LOGGER.info("Custom query extraction completed for cubes: {}", cubeConfigs.values());
            
        } catch (Exception e) {
            LOGGER.error("Unexpected error during custom query extraction", e);
        }
    }
    
    private Map<String, String> getCubeConfigurations(Properties systemsProps) {
        Map<String, String> cubeConfigs = new HashMap<>();
        
        for (String propertyName : systemsProps.stringPropertyNames()) {
            if (propertyName.endsWith(".xmla.cube")) {
                // Extract cube identifier from property name: atscale.{cubeIdentifier}.xmla.cube
                String cubeIdentifier = propertyName.substring("atscale.".length(), propertyName.length() - ".xmla.cube".length());
                // Get the actual cube name from the property value
                String actualCubeName = systemsProps.getProperty(propertyName);
                cubeConfigs.put(cubeIdentifier, actualCubeName);
                LOGGER.info("Found cube configuration - Identifier: '{}', Cube Name: '{}'", cubeIdentifier, actualCubeName);
            }
        }
        
        return cubeConfigs;
    }
    
    private boolean extractQueriesForCube(String actualCubeName, QueryHistoryFileUtil queryExtractor, Properties systemsProps) {
        LOGGER.info("Step 4.1: Processing cube: {}", actualCubeName);
        
        try {
            boolean jdbcSuccess = false;
            boolean xmlaSuccess = false;
            
            // Step 4.2: Extract JDBC queries
            LOGGER.info("Step 4.2: Loading JDBC query for cube: {}", actualCubeName);
            String jdbcQuery = loadQueryFromFile("custom_query.jdbc");
            if (jdbcQuery != null && !jdbcQuery.trim().isEmpty()) {
                LOGGER.info("JDBC query loaded, length: {} characters", jdbcQuery.length());
                LOGGER.info("Extracting JDBC queries for cube: {}", actualCubeName);
                
                try {
                    // Use actualCubeName for both property lookup and SQL query parameter
                    queryExtractor.cacheJdbcQueries(actualCubeName, jdbcQuery, "sql", actualCubeName);
                    jdbcSuccess = true;
                    LOGGER.info("JDBC query extraction successful for cube: {}", actualCubeName);
                } catch (Exception e) {
                    LOGGER.error("JDBC query extraction failed for cube: {}", actualCubeName, e);
                }
            } else {
                LOGGER.warn("No JDBC query found or query is empty for cube: {}", actualCubeName);
            }
            
            // Step 4.3: Extract XMLA queries
            LOGGER.info("Step 4.3: Loading XMLA query for cube: {}", actualCubeName);
            String xmlaQuery = loadQueryFromFile("custom_query.xmla");
            if (xmlaQuery != null && !xmlaQuery.trim().isEmpty()) {
                LOGGER.info("XMLA query loaded, length: {} characters", xmlaQuery.length());
                LOGGER.info("Extracting XMLA queries for cube: {}", actualCubeName);
                
                try {
                    // Use actualCubeName for both property lookup and SQL query parameter
                    queryExtractor.cacheXmlaQueries(actualCubeName, xmlaQuery, "analysis", actualCubeName);
                    xmlaSuccess = true;
                    LOGGER.info("XMLA query extraction successful for cube: {}", actualCubeName);
                } catch (Exception e) {
                    LOGGER.error("XMLA query extraction failed for cube: {}", actualCubeName, e);
                }
            } else {
                LOGGER.warn("No XMLA query found or query is empty for cube: {}", actualCubeName);
            }
            
            boolean overallSuccess = jdbcSuccess || xmlaSuccess; // Consider success if at least one worked
            LOGGER.info("Cube {} result: JDBC={}, XMLA={}, Overall={}", actualCubeName, jdbcSuccess, xmlaSuccess, overallSuccess);
            
            return overallSuccess;
            
        } catch (Exception e) {
            LOGGER.error("Error extracting custom queries for cube: {}", actualCubeName, e);
            return false;
        }
    }
    
    private String loadQueryFromFile(String fileName) {
        LOGGER.info("Loading query file: {}", fileName);
        
        try {
            File queryFile = new File(WORKING_DIR, CONFIG_DIR + File.separator + fileName);
            LOGGER.info("Looking for file at: {}", queryFile.getAbsolutePath());
            
            if (queryFile.exists()) {
                String query = new String(Files.readAllBytes(Paths.get(queryFile.getAbsolutePath())), StandardCharsets.UTF_8);
                LOGGER.info("Successfully loaded {}, size: {} characters", fileName, query.length());
                
                // Log first 200 characters of query for verification
                String preview = query.length() > 200 ? query.substring(0, 200) + "..." : query;
                LOGGER.info("Query preview: {}", preview.replace("\n", " ").replace("\r", ""));
                
                return query.trim();
            } else {
                LOGGER.warn("Query file not found: {}", queryFile.getAbsolutePath());
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error loading query file: {}", fileName, e);
            return null;
        }
    }
    
    private Map<String, String> loadConnectionDetails() {
        LOGGER.info("Loading connection details from config.json");
        Map<String, String> connectionDetails = new HashMap<>();
        try {
            File connectionFile = new File("config.json");
            if (!connectionFile.exists()) {
                LOGGER.error("config.json file not found at: {}", connectionFile.getAbsolutePath());
                return connectionDetails;
            }
            
            LOGGER.info("Found config.json at: {}", connectionFile.getAbsolutePath());
            String jsonContent = new String(Files.readAllBytes(Paths.get("config.json")), StandardCharsets.UTF_8);
            
            // Simple JSON parsing for basic structure
            jsonContent = jsonContent.replace("{", "").replace("}", "").replace("\"", "").replaceAll("\\s+", "");
            String[] pairs = jsonContent.split(",");
            
            LOGGER.info("Parsing {} connection properties", pairs.length);
            for (String pair : pairs) {
                String[] keyValue = pair.split(":");
                if (keyValue.length == 2) {
                    connectionDetails.put(keyValue[0].trim(), keyValue[1].trim());
                    LOGGER.info("Loaded property: {} = {}", keyValue[0].trim(), keyValue[1].trim());
                }
            }
            
            LOGGER.info("Successfully loaded {} connection properties", connectionDetails.size());
            
        } catch (Exception e) {
            LOGGER.error("Error loading config.json", e);
        }
        return connectionDetails;
    }
    
    private Properties loadSystemsProperties() {
        LOGGER.info("Loading systems.properties");
        Properties props = new Properties();
        File systemsFile = new File(WORKING_DIR, CONFIG_DIR + File.separator + "systems.properties");
        
        try (java.io.FileInputStream input = new java.io.FileInputStream(systemsFile)) {
            LOGGER.info("Found systems.properties at: {}", systemsFile.getAbsolutePath());
            props.load(new java.io.InputStreamReader(input, StandardCharsets.UTF_8));
            
            // Log cube configurations
            int cubeCount = 0;
            for (String propertyName : props.stringPropertyNames()) {
                if (propertyName.endsWith(".xmla.cube")) {
                    cubeCount++;
                    String cubeIdentifier = propertyName.substring("atscale.".length(), propertyName.length() - ".xmla.cube".length());
                    String cubeName = props.getProperty(propertyName);
                    LOGGER.info("Found cube configuration - Identifier: '{}', Cube Name: '{}'", cubeIdentifier, cubeName);
                }
            }
            
            LOGGER.info("Successfully loaded systems.properties with {} properties ({} cube configurations)", props.size(), cubeCount);
            
        } catch (Exception e) {
            LOGGER.error("Error loading systems.properties from: {}", systemsFile.getAbsolutePath(), e);
        }
        return props;
    }
    
    public String getExecutorName() {
        return "CustomQueryExtractExecutor";
    }
}