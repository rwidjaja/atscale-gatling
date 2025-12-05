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
            org.apache.logging.log4j.LogManager.shutdown();
        }
    }

    public void extractCustomQueries() {
        LOGGER.info("Step 1: Starting custom query extraction process");

        try {
            Properties systemsProps = loadSystemsProperties();
            Map<String, String> cubeConfigs = getCubeConfigurations(systemsProps);

            if (cubeConfigs.isEmpty()) {
                LOGGER.error("No cube configurations found in systems.properties");
                return;
            }

            QueryHistoryFileUtil queryExtractor = new QueryHistoryFileUtil(AtScalePostgresDao.getInstance());

            int successCount = 0;
            int errorCount = 0;

            for (Map.Entry<String, String> cubeEntry : cubeConfigs.entrySet()) {
                String cubeIdentifier = cubeEntry.getKey();
                String actualCubeName = cubeEntry.getValue();

                LOGGER.info("Processing cube - Identifier: '{}', Actual Name: '{}'", cubeIdentifier, actualCubeName);
                boolean success = extractQueriesForCube(actualCubeName, queryExtractor);
                if (success) {
                    successCount++;
                } else {
                    errorCount++;
                }
            }

            LOGGER.info("Extraction Summary: {} successful, {} failed out of {} total cubes",
                        successCount, errorCount, cubeConfigs.size());

        } catch (Exception e) {
            LOGGER.error("Unexpected error during custom query extraction", e);
        }
    }

    private Map<String, String> getCubeConfigurations(Properties systemsProps) {
        Map<String, String> cubeConfigs = new HashMap<>();
        for (String propertyName : systemsProps.stringPropertyNames()) {
            if (propertyName.endsWith(".xmla.cube")) {
                String cubeIdentifier = propertyName.substring("atscale.".length(),
                        propertyName.length() - ".xmla.cube".length());
                String actualCubeName = systemsProps.getProperty(propertyName);
                cubeConfigs.put(cubeIdentifier, actualCubeName);
                LOGGER.info("Found cube configuration - Identifier: '{}', Cube Name: '{}'", cubeIdentifier, actualCubeName);
            }
        }
        return cubeConfigs;
    }

    private boolean extractQueriesForCube(String actualCubeName, QueryHistoryFileUtil queryExtractor) {
        try {
            boolean jdbcSuccess = false;
            boolean xmlaSuccess = false;

            // JDBC: load query with ? placeholder and bind "pgsql"
            String jdbcQuery = loadQueryFromFile("custom_query.jdbc");
            if (jdbcQuery != null && !jdbcQuery.trim().isEmpty()) {
                try {
                    queryExtractor.cacheJdbcQueries(actualCubeName, jdbcQuery, "pgsql", actualCubeName);
                    jdbcSuccess = true;
                    LOGGER.info("JDBC query extraction successful for cube: {}", actualCubeName);
                } catch (Exception e) {
                    LOGGER.error("JDBC query extraction failed for cube: {}", actualCubeName, e);
                }
            } else {
                LOGGER.warn("No JDBC query found or query is empty for cube: {}", actualCubeName);
            }

            // XMLA: load query with ? placeholder and bind "analysis"
            String xmlaQuery = loadQueryFromFile("custom_query.xmla");
            if (xmlaQuery != null && !xmlaQuery.trim().isEmpty()) {
                try {
                    queryExtractor.cacheXmlaQueries(actualCubeName, xmlaQuery, "analysis", actualCubeName);
                    xmlaSuccess = true;
                    LOGGER.info("XMLA query extraction successful for cube: {}", actualCubeName);
                } catch (Exception e) {
                    LOGGER.error("XMLA query extraction failed for cube: {}", actualCubeName, e);
                }
            } else {
                LOGGER.warn("No XMLA query found or query is empty for cube: {}", actualCubeName);
            }

            return jdbcSuccess || xmlaSuccess;

        } catch (Exception e) {
            LOGGER.error("Error extracting custom queries for cube: {}", actualCubeName, e);
            return false;
        }
    }

    private String loadQueryFromFile(String fileName) {
        try {
            File queryFile = new File(WORKING_DIR, CONFIG_DIR + File.separator + fileName);
            if (queryFile.exists()) {
                String query = new String(Files.readAllBytes(Paths.get(queryFile.getAbsolutePath())),
                                          StandardCharsets.UTF_8);
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

    private Properties loadSystemsProperties() {
        Properties props = new Properties();
        File systemsFile = new File(WORKING_DIR, CONFIG_DIR + File.separator + "systems.properties");
        try (java.io.FileInputStream input = new java.io.FileInputStream(systemsFile)) {
            props.load(new java.io.InputStreamReader(input, StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOGGER.error("Error loading systems.properties from: {}", systemsFile.getAbsolutePath(), e);
        }
        return props;
    }

    public String getExecutorName() {
        return "CustomQueryExtractExecutor";
    }
}
