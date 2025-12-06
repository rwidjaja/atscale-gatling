package executors;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.utils.PropertiesManager;

import utils.RunLogUtils;

public class ArchiveXmlaToSnowflakeExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveXmlaToSnowflakeExecutor.class);
    private static final String STAGE = "XMLA_LOGS_STAGE";
    private static final String RAW_TABLE = "GATLING_RAW_XMLA_LOGS";
    private static final String WORKING_DIR = "working_dir";
    private static final String RUN_LOGS_DIR = WORKING_DIR + "/run_logs";
    private static final String CONFIG_DIR = WORKING_DIR + "/config";
    private static final String SYSTEMS_PROPERTIES = CONFIG_DIR + "/systems.properties";

    static {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            LOGGER.info("Snowflake JDBC driver registered in static initializer");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Failed to load Snowflake JDBC driver: {}", e.getMessage());
            throw new RuntimeException("Snowflake JDBC driver not found. Make sure snowflake-jdbc dependency is included in pom.xml", e);
        }
    }

    public static void main(String[] args) {
        LOGGER.info("ArchiveXmlaToSnowflakeExecutor started.");
        try {
            // Initialize properties from systems.properties FIRST
            loadSystemsProperties();
            
            ArchiveXmlaToSnowflakeExecutor executor = new ArchiveXmlaToSnowflakeExecutor();
            executor.initAdditionalProperties();
            executor.execute();
        } catch (Exception e) {
            LOGGER.error("Error during ArchiveXmlaToSnowflakeExecutor execution", e);
            throw new RuntimeException("ArchiveXmlaToSnowflakeExecutor failed", e);
        }
        LOGGER.info("ArchiveXmlaToSnowflakeExecutor completed.");
        try {
            Thread.sleep(Duration.ofSeconds(10).toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        org.apache.logging.log4j.LogManager.shutdown();
    }

    private static void loadSystemsProperties() {
        try {
            Path propertiesPath = Paths.get(SYSTEMS_PROPERTIES);
            if (Files.exists(propertiesPath)) {
                LOGGER.info("Loading properties from: {}", propertiesPath.toAbsolutePath());
                Properties properties = new Properties();
                try (java.io.FileInputStream fis = new java.io.FileInputStream(propertiesPath.toFile())) {
                    properties.load(fis);
                }
                
                // Convert Properties to Map for PropertiesManager
                Map<String, String> propsMap = new HashMap<>();
                for (String key : properties.stringPropertyNames()) {
                    propsMap.put(key, properties.getProperty(key));
                }
                
                // Set properties in PropertiesManager
                PropertiesManager.setCustomProperties(propsMap);
                LOGGER.info("Successfully loaded systems.properties with {} properties", propsMap.size());
            } else {
                LOGGER.warn("Properties file not found: {}", propertiesPath.toAbsolutePath());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to load systems.properties", e);
        }
    }

    protected void execute() {
        // Get ALL cube names from systems.properties
        List<String> cubeNames = getAllCubeNamesFromProperties();
        if (cubeNames.isEmpty()) {
            LOGGER.warn("No cubes found in systems.properties. Nothing to archive.");
            return;
        }
        
        LOGGER.info("Found {} cubes in systems.properties: {}", cubeNames.size(), cubeNames);
        
        // Find XMLA log files for these cubes
        List<Path> xmlaLogFiles = findXmlaLogFiles(cubeNames);
        if (xmlaLogFiles.isEmpty()) {
            LOGGER.warn("No XMLA log files found for cubes. Nothing to archive.");
            return;
        }
        
        LOGGER.info("Found {} XMLA log files to archive: {}", xmlaLogFiles.size(), xmlaLogFiles);
        
        String jdbcUrl = getSnowflakeURL();
        Properties connectionProps = getConnectionProperties();

        LOGGER.info("Connecting to Snowflake with URL: {}", jdbcUrl);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            LOGGER.info("Connected to Snowflake successfully.");

            // Ensure required objects exist
            createIfNotExistsObjects(conn);
            conn.commit();

            // Process each XMLA log file
            for (Path dataFile : xmlaLogFiles) {
                processXmlaLogFile(conn, dataFile);
            }

            LOGGER.info("✅ All XMLA log files archived successfully.");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute Snowflake operations", e);
        }
    }

    private List<String> getAllCubeNamesFromProperties() {
        Set<String> cubeNames = new HashSet<>();
        try {
            // Load properties directly from systems.properties
            Path propertiesPath = Paths.get(SYSTEMS_PROPERTIES);
            if (Files.exists(propertiesPath)) {
                Properties properties = new Properties();
                try (java.io.FileInputStream fis = new java.io.FileInputStream(propertiesPath.toFile())) {
                    properties.load(fis);
                    
                    // Look for cube properties like in the simulation executor
                    for (String propertyName : properties.stringPropertyNames()) {
                        if (propertyName.startsWith("atscale.") && propertyName.contains(".xmla.cube")) {
                            String cubeIdentifier = propertyName.substring("atscale.".length(), propertyName.indexOf(".xmla.cube"));
                            String cubeName = properties.getProperty(propertyName);
                            if (cubeName != null && !cubeName.trim().isEmpty()) {
                                cubeNames.add(cubeName.trim());
                                LOGGER.debug("Found cube '{}' from property '{}'", cubeName, propertyName);
                            }
                        }
                    }
                    
                    // Fallback to models property if no specific cubes found
                    if (cubeNames.isEmpty()) {
                        String models = properties.getProperty("atscale.models");
                        if (models != null && !models.trim().isEmpty()) {
                            String[] modelList = models.split(",");
                            for (String model : modelList) {
                                String trimmedModel = model.trim();
                                if (!trimmedModel.isEmpty()) {
                                    cubeNames.add(trimmedModel);
                                    LOGGER.debug("Found cube '{}' from atscale.models", trimmedModel);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading cube names from properties", e);
        }
        
        LOGGER.info("Retrieved {} cube names from properties", cubeNames.size());
        return new ArrayList<>(cubeNames);
    }

    private List<Path> findXmlaLogFiles(List<String> cubeNames) {
        List<Path> xmlaLogFiles = new ArrayList<>();
        Path runLogsDir = Paths.get(RUN_LOGS_DIR);
        
        if (!Files.exists(runLogsDir) || !Files.isDirectory(runLogsDir)) {
            LOGGER.warn("Run logs directory does not exist: {}", RUN_LOGS_DIR);
            return xmlaLogFiles;
        }

        try {
            // Look for XMLA log files for each cube
            for (String cubeName : cubeNames) {
                // ONLY replace spaces with underscores, keep dashes as-is
                String sanitizedCubeName = cubeName.replace(" ", "_");
                
                LOGGER.debug("Looking for XMLA log files for cube: {} (sanitized: {})", cubeName, sanitizedCubeName);
                
                // Look for files that match the pattern: <sanitized_cube>_xmla*.log
                try (var stream = Files.list(runLogsDir)) {
                    stream.filter(Files::isRegularFile)
                        .filter(path -> {
                            String fileName = path.getFileName().toString();
                            // Look for files starting with sanitized cube name, containing _xmla, ending with .log
                            return fileName.startsWith(sanitizedCubeName + "_") && 
                                    fileName.contains("_xmla") &&
                                    fileName.endsWith(".log");
                        })
                        .forEach(xmlaLogFile -> {
                            xmlaLogFiles.add(xmlaLogFile);
                            LOGGER.info("Found XMLA log file for cube '{}': {}", cubeName, xmlaLogFile.getFileName());
                        });
                }
            }

            // Also look for any XMLA log files that might have different naming patterns
            LOGGER.debug("Looking for additional XMLA log files with broader pattern...");
            try (var stream = Files.list(runLogsDir)) {
                stream.filter(Files::isRegularFile)
                    .filter(path -> {
                        String fileName = path.getFileName().toString();
                        // Look for files containing "_xmla_" and ".log"
                        return fileName.contains("_xmla_") && 
                                fileName.endsWith(".log") &&
                                !xmlaLogFiles.contains(path);
                    })
                    .forEach(xmlaLogFile -> {
                        xmlaLogFiles.add(xmlaLogFile);
                        LOGGER.info("Found additional XMLA log file: {}", xmlaLogFile.getFileName());
                    });
            }

        } catch (Exception e) {
            LOGGER.error("Error searching for XMLA log files", e);
        }

        LOGGER.info("Total XMLA log files found: {}", xmlaLogFiles.size());
        return xmlaLogFiles;
    }

    private void processXmlaLogFile(Connection conn, Path dataFile) {
        LOGGER.info("Processing XMLA log file: {}", dataFile);
        
        try {
            List<String> runIds = RunLogUtils.extractGatlingRunIds(dataFile);
            LOGGER.info("Found {} unique RUN IDs in XMLA log file {}: {}", runIds.size(), dataFile, runIds);

            boolean originalAutoCommit = conn.getAutoCommit();

            String stagedFileName = String.format("%s.%s",
                    dataFile.getFileName().toString().replace("'", "''"), "gz");
            String fileUri = dataFile.toUri().toString().replace("'", "''");

            try {
                try {
                    // 0) Ensure required objects exist
                    createIfNotExistsObjects(conn);
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }

                // 1) Upload local file to stage (PUT is not transactional)
                exec(conn, "PUT '" + fileUri + "' @" + STAGE + " AUTO_COMPRESS=TRUE OVERWRITE=TRUE");
                LOGGER.info("Uploaded file {} to stage {} as {}", fileUri, STAGE, stagedFileName);

                // Begin DML transaction
                conn.setAutoCommit(false);

                // 2) Clean up any prior data for the same run IDs (idempotency step)
                if (!runIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(getCleanRawLogsForRunIdSql())) {
                        final int batchSize = 1000;
                        int count = 0;
                        for (String runId : runIds) {
                            String pattern = "%gatlingRunId='" + runId + "'%";
                            ps.setString(1, pattern);
                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                                LOGGER.debug("Deleted raw log batch of {} runIds", batchSize);
                            }
                        }
                        if (count % batchSize != 0) {
                            ps.executeBatch();
                            LOGGER.debug("Deleted final raw log batch of {} runIds", count % batchSize);
                        }
                    }
                }

                // 3) COPY into RAW table (fail the whole transaction on any load error)
                exec(conn, getCopyIntoRawSql(stagedFileName));
                LOGGER.info("Copied data from stage {} into table {}", STAGE, RAW_TABLE);
                conn.commit();

                // 4) Copy into the HEADERS table this step is idempotent
                if (!runIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(getInsertIntoHeadersSql())) {
                        final int batchSize = 1000;
                        int count = 0;
                        for (String runId : runIds) {
                            String pattern = "%gatlingRunId='" + runId + "'%";
                            ps.setString(1, pattern);
                            ps.setString(2, runId);
                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                                LOGGER.debug("Inserted header batch of {} runIds", batchSize);
                            }
                        }
                        if (count % batchSize != 0) {
                            ps.executeBatch();
                            LOGGER.debug("Inserted final header batch of {} runIds", count % batchSize);
                        }
                    }
                    LOGGER.info("Inserted header rows into GATLING_XMLA_HEADERS from gatling_raw_xmla_logs");
                }

                // 5) Copy into the RESPONSES table this step is idempotent
                if (!runIds.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(getInsertIntoResponsesSql())) {
                        final int batchSize = 1000;
                        int count = 0;
                        for (String runId : runIds) {
                            ps.setString(1, runId);
                            ps.setString(2, runId);
                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                                LOGGER.debug("Inserted responses batch of {} runIds", batchSize);
                            }
                        }
                        if (count % batchSize != 0) {
                            ps.executeBatch();
                            LOGGER.debug("Inserted final responses batch of {} runIds", count % batchSize);
                        }
                    }
                    LOGGER.info("Inserted response rows into gatling_xmla_responses");
                }

                // cleanup staged file
                try {
                    exec(conn, "REMOVE @" + STAGE + "/" + stagedFileName);
                } catch (SQLException cleanupEx) {
                    LOGGER.warn("Failed to remove staged file {}: {}", stagedFileName, cleanupEx.getMessage());
                }

                LOGGER.info("✅ XMLA Load complete for file: {}", dataFile.getFileName());
            } catch (SQLException e) {
                try {
                    if (!conn.getAutoCommit()) conn.rollback();
                } catch (SQLException rbEx) {
                    LOGGER.error("Rollback failed: {}", rbEx.getMessage());
                }
                try {
                    exec(conn, "REMOVE @" + STAGE + "/" + stagedFileName);
                } catch (SQLException cleanupEx) {
                    LOGGER.warn("Failed to remove staged file after rollback {}: {}", stagedFileName, cleanupEx.getMessage());
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(originalAutoCommit);
                } catch (SQLException ex) {
                    LOGGER.warn("Failed to restore auto-commit: {}", ex.getMessage());
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to process XMLA log file: {}", dataFile, e);
            // Continue with next file even if this one fails
        }
    }

    /** Create stage, file format, and XMLA tables. */
    private static void createIfNotExistsObjects(Connection conn) throws SQLException {
        LOGGER.info("Ensuring all required Snowflake objects for XMLA exist...");

        exec(conn, """
            CREATE STAGE IF NOT EXISTS XMLA_LOGS_STAGE
              FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\t');
            """);

        exec(conn, """
            CREATE FILE FORMAT IF NOT EXISTS XMLA_WHOLE_LINE_FMT
              TYPE = 'CSV'
              FIELD_DELIMITER = '\\t'
              SKIP_HEADER = 0
              TRIM_SPACE = FALSE
              FIELD_OPTIONALLY_ENCLOSED_BY = NONE
              EMPTY_FIELD_AS_NULL = FALSE
              NULL_IF = ();
            """);

        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_RAW_XMLA_LOGS (
              RAW_SOAP VARCHAR(16777216),
              SRC_FILENAME VARCHAR(16777216),
              SRC_ROW_NUMBER NUMBER(38,0)
            );
            """);

        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_XMLA_HEADERS CLUSTER BY (GATLING_RUN_ID) (
              RUN_KEY NUMBER(19,0),
              TS TIMESTAMP_NTZ(9),
              LEVEL VARCHAR(16777216),
              LOGGER VARCHAR(16777216),
              MESSAGE_KIND VARCHAR(16777216),
              GATLING_RUN_ID VARCHAR(512) NOT NULL,
              STATUS VARCHAR(12),
              GATLING_SESSION_ID NUMBER(8,0),
              MODEL VARCHAR(1024),
              CUBE VARCHAR(1024),
              CATALOG VARCHAR(1024),
              QUERY_NAME VARCHAR(1024),
              QUERY_HASH VARCHAR(256),
              START_MS NUMBER(38,0),
              END_MS NUMBER(38,0),
              DURATION_MS NUMBER(38,0),
              RESPONSE_SIZE NUMBER(38,0),
              RAW_SOAP VARCHAR(16777216),
              PRIMARY KEY (RUN_KEY)
            );
            """);

        exec(conn, """
            CREATE TABLE IF NOT EXISTS GATLING_XMLA_RESPONSES CLUSTER BY (GATLING_RUN_ID)(
              RUN_KEY NUMBER(19,0),
              GATLING_RUN_ID VARCHAR(512),
              STATUS VARCHAR(12),
              GATLING_SESSION_ID NUMBER(8,0),
              MODEL VARCHAR(1024),
              CUBE VARCHAR(1024),
              CATALOG VARCHAR(1024),
              QUERY_NAME VARCHAR(1024),
              QUERY_HASH VARCHAR(256),
              SOAP_HEADER VARIANT,
              SOAP_BODY VARIANT,
              SOAP_BODY_HASH VARCHAR(256),
              PRIMARY KEY (RUN_KEY)
            );
            """);

        LOGGER.info("✅ All required XMLA Snowflake schema objects verified.");
    }

    private static String getCopyIntoRawSql(String fileName) {
        return String.format("""
              COPY INTO GATLING_RAW_XMLA_LOGS (RAW_SOAP, SRC_FILENAME, SRC_ROW_NUMBER)
              FROM (
                SELECT
                  $1 AS RAW_SOAP,
                  METADATA$FILENAME AS SRC_FILENAME,
                  METADATA$FILE_ROW_NUMBER AS SRC_ROW_NUMBER
                FROM @XMLA_LOGS_STAGE
              )
              FILES = ('%s')
              FILE_FORMAT = (FORMAT_NAME = XMLA_WHOLE_LINE_FMT)
              PURGE=TRUE
              ON_ERROR = 'ABORT_STATEMENT';
            """, fileName);
    }

    private static String getCleanRawLogsForRunIdSql() {
        return """
            DELETE FROM GATLING_RAW_XMLA_LOGS
            WHERE RAW_SOAP LIKE ?
            """;
    }

    private static String getInsertIntoHeadersSql() {
        return """
            INSERT INTO GATLING_XMLA_HEADERS (
                    RUN_KEY,
                    TS,
                    LEVEL,
                    LOGGER,
                    MESSAGE_KIND,
                    GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    QUERY_HASH,
                    START_MS,
                    END_MS,
                    DURATION_MS,
                    RESPONSE_SIZE,
                    RAW_SOAP
                )
                WITH ParsedData AS (
                    SELECT
                        to_timestamp_ntz(regexp_substr(raw_soap, '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}')) AS TS,
                        regexp_substr(raw_soap, '^[^ ]+ [^ ]+ ([A-Z]+)', 1, 1, 'e', 1) AS LEVEL,
                        regexp_replace(regexp_substr(raw_soap, ' [A-Za-z0-9_\\\\\\\\\\\\\\\\.]+:', 1, 1), '[: ]', '') AS LOGGER,
                        regexp_substr(raw_soap, '- ([A-Za-z0-9_]+)', 1, 1, 'e', 1) AS MESSAGE_KIND,
                        regexp_substr(raw_soap, 'gatlingRunId=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS GATLING_RUN_ID,
                        regexp_substr(raw_soap, 'status=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS STATUS,
                        regexp_substr(raw_soap, 'gatlingSessionId=([^\\\\s]+)', 1, 1, 'e', 1) AS GATLING_SESSION_ID,
                        regexp_substr(raw_soap, 'model=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS MODEL,
                        regexp_substr(raw_soap, 'cube=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS CUBE,
                        regexp_substr(raw_soap, 'catalog=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS CATALOG,
                        regexp_substr(raw_soap, 'queryName=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS QUERY_NAME,
                        regexp_substr(raw_soap, 'atscaleQueryId=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS ATSCALE_QUERY_ID,
                        regexp_substr(raw_soap, 'inboundTextAsHash=\\'([^\\']+)\\'', 1, 1, 'e', 1) AS QUERY_HASH,
                        regexp_substr(raw_soap, 'start=([^\\\\s]+)', 1, 1, 'e', 1) AS START_MS,
                        regexp_substr(raw_soap, 'end=([^\\\\s]+)', 1, 1, 'e', 1) AS END_MS,
                        regexp_substr(raw_soap, 'duration=([^\\\\s]+)', 1, 1, 'e', 1) AS DURATION_MS,
                        regexp_substr(raw_soap, 'responseSize=([^\\\\s]+)', 1, 1, 'e', 1) AS RESPONSE_SIZE,
                        regexp_substr(raw_soap, '<soap:Envelope.*</soap:Envelope>', 1, 1, 's') AS RAW_SOAP
                    FROM
                        GATLING_RAW_XMLA_LOGS AS UPLOAD
                    WHERE
                        UPLOAD.RAW_SOAP LIKE ?
                        AND NOT EXISTS (
                            select gatling_run_id from gatling_xmla_headers
                            where gatling_run_id = ?
                            limit 1
                        )
                )
                SELECT
                    HASH(GATLING_RUN_ID, GATLING_SESSION_ID, MODEL, QUERY_HASH) AS RUN_KEY,
                    TS,
                    LEVEL,
                    LOGGER,
                    MESSAGE_KIND,
                    TRIM(GATLING_RUN_ID) as GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    QUERY_HASH,
                    START_MS,
                    END_MS,
                    DURATION_MS,
                    RESPONSE_SIZE,
                    RAW_SOAP
                FROM
                    ParsedData
                ORDER BY
                    MODEL, CUBE, CATALOG, QUERY_NAME;
            """;
    }

    private static String getInsertIntoResponsesSql() {
        return """
               INSERT INTO GATLING_XMLA_RESPONSES (
                    RUN_KEY,
                    GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    QUERY_HASH,
                    SOAP_HEADER,
                    SOAP_BODY,
                    SOAP_BODY_HASH
                )
                WITH ModifiedSoap AS (
                    SELECT
                        *,
                        REGEXP_REPLACE(
                          XMLGET(PARSE_XML(RAW_SOAP), 'soap:Body')::VARCHAR,
                          '<LastDataUpdate.*?>[^<]*</LastDataUpdate>',
                          '<LastDataUpdate>0</LastDataUpdate>'
                        ) AS MODIFIED_SOAP_BODY_STR
                    FROM
                        GATLING_XMLA_HEADERS
                    WHERE
                        GATLING_RUN_ID = ?
                        AND NOT EXISTS (
                              select gatling_run_id from gatling_xmla_responses
                              where gatling_run_id = ?
                              limit 1
                         )
                    )
                SELECT
                    RUN_KEY,
                    GATLING_RUN_ID,
                    STATUS,
                    GATLING_SESSION_ID,
                    MODEL,
                    CUBE,
                    CATALOG,
                    QUERY_NAME,
                    QUERY_HASH,
                    XMLGET(PARSE_XML(RAW_SOAP),'soap:Header') AS SOAP_HEADER,
                    MODIFIED_SOAP_BODY_STR::VARIANT AS SOAP_BODY,
                    SHA2(MODIFIED_SOAP_BODY_STR, 256) AS SOAP_BODY_HASH
                FROM
                    ModifiedSoap;
               """;
    }

    private static void exec(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private Properties getConnectionProperties() {
        // Use the public methods available in PropertiesManager
        String user = PropertiesManager.getCustomProperty("snowflake.archive.username");
        String token = PropertiesManager.getCustomProperty("snowflake.archive.token");
        String warehouse = PropertiesManager.getCustomProperty("snowflake.archive.warehouse");
        String database = PropertiesManager.getCustomProperty("snowflake.archive.database");
        String schema = PropertiesManager.getCustomProperty("snowflake.archive.schema");
        String role = null;

        // Check if role property exists using the public method
        if (PropertiesManager.hasProperty("snowflake.archive.role")) {
            role = PropertiesManager.getCustomProperty("snowflake.archive.role");
        }

        // Defensive: trim values read from properties to remove accidental whitespace/newlines
        if (user != null) user = user.trim();
        if (token != null) token = token.trim();

        Properties props = new Properties();
        props.put("user", user);
        props.put("warehouse", warehouse);
        props.put("db", database);
        props.put("schema", schema);
        
        // Use token as the password for OAuth authentication
        if (token != null && !token.isEmpty()) {
            props.put("password", token);
            LOGGER.debug("Using OAuth token as password");
        } else {
            throw new RuntimeException("Token is not provided for Snowflake OAuth authentication");
        }
        
        // Add role if specified
        if (role != null && !role.trim().isEmpty()) {
            props.put("role", role.trim());
        }

        // Debug: log username and authentication method
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Snowflake connection properties: user='{}', authentication=OAuth token, warehouse='{}', database='{}', schema='{}'", 
                        user, warehouse, database, schema);
        }

        return props;
    }

    private String getSnowflakeURL() {
        // Use the public method getCustomProperty instead of the private getProperty
        String account = PropertiesManager.getCustomProperty("snowflake.archive.account");
        return String.format("jdbc:snowflake://%s.snowflakecomputing.com/", account);
    }

    protected void initAdditionalProperties() {
        AdditionalPropertiesLoader loader = new AdditionalPropertiesLoader();
        PropertiesManager.setCustomProperties(loader.fetchAdditionalProperties(AdditionalPropertiesLoader.SecretsManagerType.AWS));
    }
}