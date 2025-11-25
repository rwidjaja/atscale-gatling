package executors;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.dao.AtScalePostgresDao;
import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.utils.QueryHistoryFileUtil;


public class InstallerVerQueryExtractExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallerVerQueryExtractExecutor.class);
    private static final String WORKING_DIR = "working_dir";
    private static final String CONFIG_DIR = "config";
    private static final String BASE_QUERY_FILE = "base_query.sql";

    public static void main(String[] args) {
        InstallerVerQueryExtractExecutor executor = new InstallerVerQueryExtractExecutor();
        executor.initAdditionalProperties();
        executor.execute();
    }

    protected void execute() {
        LOGGER.info("QueryExtractExecutor started.");
        
        // Load the base query from file
        String baseQuery = loadBaseQuery();
        if (baseQuery == null || baseQuery.trim().isEmpty()) {
            LOGGER.error("Failed to load base query from file: {}/{}/{}", WORKING_DIR, CONFIG_DIR, BASE_QUERY_FILE);
            return;
        }
        
        LOGGER.info("Loaded base query from file, length: {} characters", baseQuery.length());
        
        // Get cube configurations instead of models
        Map<String, CubeConfig> cubeConfigs = getCubeConfigurations();
        
        for (Map.Entry<String, CubeConfig> cubeEntry : cubeConfigs.entrySet()) {
            String cubeIdentifier = cubeEntry.getKey();
            CubeConfig config = cubeEntry.getValue();
            
            LOGGER.info("Processing cube - Identifier: {}, Cube Name: {}, Catalog: {}", 
                       cubeIdentifier, config.cubeName, config.catalogName);
            
            cacheJdbcQueries(config.cubeName, baseQuery);
            cacheXmlaQueries(config.cubeName, baseQuery);
        }

        LOGGER.info("QueryExtractExecutor finished.");
        org.apache.logging.log4j.LogManager.shutdown();
    }

    private String loadBaseQuery() {
        try {
            java.io.File queryFile = new java.io.File(WORKING_DIR, CONFIG_DIR + java.io.File.separator + BASE_QUERY_FILE);
            LOGGER.info("Loading base query from: {}", queryFile.getAbsolutePath());
            
            if (queryFile.exists()) {
                String query = new String(Files.readAllBytes(Paths.get(queryFile.getAbsolutePath())), StandardCharsets.UTF_8);
                LOGGER.info("Successfully loaded base query, size: {} characters", query.length());
                return query.trim();
            } else {
                LOGGER.error("Base query file not found: {}", queryFile.getAbsolutePath());
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error loading base query from file", e);
            return null;
        }
    }

    private Map<String, CubeConfig> getCubeConfigurations() {
        Map<String, CubeConfig> cubeConfigs = new HashMap<>();
        
        // Get all cube configurations by looking for properties that contain .xmla.cube
        java.util.Properties props = getProperties();
        for (String propertyName : props.stringPropertyNames()) {
            if (propertyName.startsWith("atscale.") && propertyName.contains(".xmla.cube")) {
                // Extract cube identifier (the part between "atscale." and ".xmla.cube")
                String cubeIdentifier = propertyName.substring("atscale.".length(), propertyName.indexOf(".xmla.cube"));
                String cubeName = props.getProperty(propertyName);
                
                // Get the catalog name for this cube
                String catalogProperty = "atscale." + cubeIdentifier + ".xmla.catalog";
                String catalogName = props.getProperty(catalogProperty);
                
                if (catalogName == null) {
                    LOGGER.warn("No catalog found for cube identifier: {}, using cube name as fallback", cubeIdentifier);
                    catalogName = cubeName;
                }
                
                CubeConfig config = new CubeConfig(cubeName, catalogName);
                cubeConfigs.put(cubeIdentifier, config);
                LOGGER.info("Found cube configuration - Identifier: {}, Cube: {}, Catalog: {}", 
                           cubeIdentifier, cubeName, catalogName);
            }
        }
        
        if (cubeConfigs.isEmpty()) {
            LOGGER.warn("No cube configurations found. Falling back to models.");
            // Fallback to models if no cube configurations found
            List<String> models = PropertiesManager.getAtScaleModels();
            for (String model : models) {
                cubeConfigs.put(model, new CubeConfig(model, model));
            }
        }
        
        return cubeConfigs;
    }

    private void cacheJdbcQueries(String cubeName, String baseQuery) {
        LOGGER.info("Caching JDBC queries for cube: {}", cubeName);

        AtScalePostgresDao dao = AtScalePostgresDao.getInstance();
        QueryHistoryFileUtil queryHistoryFileUtil = new QueryHistoryFileUtil(dao);
        
        LOGGER.info("Calling QueryHistoryFileUtil.cacheJdbcQueries with model: {}, cubeName: {}", cubeName, cubeName);
        queryHistoryFileUtil.cacheJdbcQueries(cubeName, baseQuery, AtScalePostgresDao.QueryLanguage.INSTALLER_SQL.getValue(), cubeName);
    }

    private void cacheXmlaQueries(String cubeName, String baseQuery) {
        LOGGER.info("Caching XMLA queries for cube: {}", cubeName);

        AtScalePostgresDao dao = AtScalePostgresDao.getInstance();
        QueryHistoryFileUtil queryHistoryFileUtil = new QueryHistoryFileUtil(dao);
        
        LOGGER.info("Calling QueryHistoryFileUtil.cacheXmlaQueries with model: {}, cubeName: {}", cubeName, cubeName);
        queryHistoryFileUtil.cacheXmlaQueries(cubeName, baseQuery, AtScalePostgresDao.QueryLanguage.XMLA.getValue(), cubeName);
    }

    protected void initAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        PropertiesManager.setCustomProperties(loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS));
    }

    // Helper method to get properties
    private java.util.Properties getProperties() {
        try {
            java.util.Properties props = new java.util.Properties();
            java.io.File file = new java.io.File(WORKING_DIR + java.io.File.separator + CONFIG_DIR + java.io.File.separator + "systems.properties");
            if (file.exists()) {
                try (java.io.FileInputStream input = new java.io.FileInputStream(file)) {
                    props.load(input);
                }
            }
            return props;
        } catch (Exception e) {
            LOGGER.warn("Could not load properties directly, using empty properties", e);
            return new java.util.Properties();
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