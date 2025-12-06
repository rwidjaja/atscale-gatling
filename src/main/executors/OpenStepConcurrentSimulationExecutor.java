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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.atscale.java.executors.ConcurrentSimulationExecutor;
import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.injectionsteps.AtOnceUsersOpenInjectionStep;
import com.atscale.java.injectionsteps.OpenStep;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OpenStepConcurrentSimulationExecutor extends ConcurrentSimulationExecutor<OpenStep> {

    private static final Logger logger = Logger.getLogger(OpenStepConcurrentSimulationExecutor.class.getName());
    private static final String WORKING_DIR = "working_dir";
    private static final String QUERIES_DIR = "queries";
    private static final String INGEST_DIR = "ingest";
    private static final String CONFIG_DIR = "config";

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }

    public static void main(String[] args) {
        logger.info("OpenStepConcurrentSimulationExecutor started.");
        
        // Add a shutdown hook to verify System.exit works
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("🎯 SHUTDOWN HOOK EXECUTED - System.exit() is working!");
        }));
        
        int exitCode = 0;
        ExecutorService executorService = null;
        
        try {
            // First, load the configuration to get actual duration
            // Default durationMinutes from GUI is 45 for concurrent open steps
            int simulationDurationMinutes = 1; // fallback default

            try {
                File runtimeFile = new File("working_dir", "config" + File.separator + "runtime.json");
                if (runtimeFile.exists()) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode rootNode = objectMapper.readTree(runtimeFile);
                    JsonNode executorConfig = rootNode.get("OpenStepConcurrentSimulationExecutor");
                    if (executorConfig != null) {
                        JsonNode injectionSteps = executorConfig.get("injectionSteps");
                        if (injectionSteps != null && injectionSteps.isArray() && injectionSteps.size() > 0) {
                            JsonNode firstStep = injectionSteps.get(0);
                            if (firstStep.has("durationMinutes")) {
                                int durationMinutes = firstStep.get("durationMinutes").asInt(45);
                                // ✅ Use minutes directly, enforce minimum of 1
                                simulationDurationMinutes = Math.max(1, durationMinutes);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to read runtime.json, using default timeout", e);
            }

            // Calculate timeout in seconds: simulation duration + 2-minute buffer
            long timeoutSeconds = (simulationDurationMinutes * 60L) + 120L;

            logger.info("Setting timeout to " + timeoutSeconds +
                    " seconds (simulation: " + simulationDurationMinutes + " minutes + 2-minute buffer)");


            // Create a thread pool to run the simulation with timeout
            executorService = Executors.newSingleThreadExecutor();
            Future<?> future = executorService.submit(() -> {
                OpenStepConcurrentSimulationExecutor executor = new OpenStepConcurrentSimulationExecutor();
                executor.execute();
            });
            
            try {
                future.get(timeoutSeconds, TimeUnit.SECONDS);
                logger.info("OpenStepConcurrentSimulationExecutor completed successfully.");
            } catch (TimeoutException e) {
                logger.severe("OpenStepConcurrentSimulationExecutor timed out after " + 
                             timeoutSeconds + " seconds! Forcing shutdown...");
                future.cancel(true); // Interrupt the execution
                
                // Dump threads to see what's hanging
                dumpThreads();
                
                exitCode = 1;
            } catch (ExecutionException e) {
                logger.log(Level.SEVERE, "OpenStepConcurrentSimulationExecutor failed", e.getCause());
                exitCode = 1;
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "OpenStepConcurrentSimulationExecutor interrupted", e);
                exitCode = 1;
            }
            
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "OpenStepConcurrentSimulationExecutor failed with error", t);
            exitCode = 1;
        } finally {
            // Shutdown executor service
            if (executorService != null) {
                executorService.shutdownNow();
                try {
                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executorService.shutdownNow();
                }
            }
            
            logger.info("About to call System.exit(" + exitCode + ")");
            
            // 🔥 FORCE JVM SHUTDOWN
            System.exit(exitCode);
        }
    }
    
    private static void dumpThreads() {
        try {
            Map<Thread, StackTraceElement[]> all = Thread.getAllStackTraces();
            logger.info("=== THREAD DUMP (" + all.size() + " threads) ===");
            for (Thread t : all.keySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Thread[name=").append(t.getName())
                .append(", daemon=").append(t.isDaemon())
                .append(", state=").append(t.getState())
                .append(", id=").append(t.getId())
                .append(", alive=").append(t.isAlive())
                .append("]");
                
                logger.info(sb.toString());
                
                // Only show stack traces for non-daemon threads or threads in RUNNABLE state
                if (!t.isDaemon() || t.getState() == Thread.State.RUNNABLE) {
                    StackTraceElement[] st = all.get(t);
                    for (StackTraceElement e : st) {
                        logger.info("    at " + e.toString());
                    }
                }
            }
            logger.info("=== END THREAD DUMP ===");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to dump threads", e);
        }
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

            logger.info("Created " + tasks.size() + " open step concurrent tasks for " + cubeConfigs.size() + " cubes");

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

                CubeConfig config = new CubeConfig(cubeName, catalogName);
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
                        cubeConfigs.put(trimmedModel, new CubeConfig(trimmedModel, trimmedModel));
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

            // JDBC CSV -> JSON fallback
            String jdbcCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileName");
            boolean jdbcHasHeader = Boolean.parseBoolean(
                    systemsProps.getProperty("atscale." + cubeIdentifier + ".jdbc.setIngestionFileHasHeader", "true")
            );

            if (jdbcCsvFile != null && !jdbcCsvFile.trim().isEmpty()) {
                File jdbcFile = new File(WORKING_DIR, INGEST_DIR + File.separator + jdbcCsvFile);
                if (jdbcFile.exists()) {
                    MavenTaskDto<OpenStep> jdbcTask = createTaskForCsvFile(cubeName, catalogName, jdbcFile,
                            "jdbc", jdbcConnectionDetails, jdbcHasHeader);
                    if (jdbcTask != null) cubeTasks.add(jdbcTask);
                    else logger.warning("Failed to create JDBC CSV task for cube: " + cubeName);
                } else {
                    logger.warning("JDBC CSV file not found: " + jdbcFile.getAbsolutePath());
                }
            } else {
                String jdbcFileName = cubeName + "_jdbc_queries.json";
                File jdbcFile = getQueryFile(jdbcFileName);
                if (jdbcFile != null && jdbcFile.exists()) {
                    MavenTaskDto<OpenStep> jdbcTask = createTaskForJsonFile(cubeName, catalogName, jdbcFile,
                            "jdbc", jdbcConnectionDetails);
                    if (jdbcTask != null) cubeTasks.add(jdbcTask);
                    else logger.warning("Failed to create JDBC JSON task for cube: " + cubeName);
                } else {
                    logger.fine("JDBC queries file not found for cube: " + cubeName + " (looking for: " + jdbcFileName + ")");
                }
            }

            // XMLA CSV -> JSON fallback
            String xmlaCsvFile = systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileName");
            boolean xmlaHasHeader = Boolean.parseBoolean(
                    systemsProps.getProperty("atscale." + cubeIdentifier + ".xmla.setIngestionFileHasHeader", "true")
            );

            if (xmlaCsvFile != null && !xmlaCsvFile.trim().isEmpty()) {
                File xmlaFile = new File(WORKING_DIR, INGEST_DIR + File.separator + xmlaCsvFile);
                if (xmlaFile.exists()) {
                    MavenTaskDto<OpenStep> xmlaTask = createTaskForCsvFile(cubeName, catalogName, xmlaFile,
                            "xmla", xmlaConnectionDetails, xmlaHasHeader);
                    if (xmlaTask != null) cubeTasks.add(xmlaTask);
                    else logger.warning("Failed to create XMLA CSV task for cube: " + cubeName);
                } else {
                    logger.warning("XMLA CSV file not found: " + xmlaFile.getAbsolutePath());
                }
            } else {
                String xmlaFileName = cubeName + "_xmla_queries.json";
                File xmlaFile = getQueryFile(xmlaFileName);
                if (xmlaFile != null && xmlaFile.exists()) {
                    MavenTaskDto<OpenStep> xmlaTask = createTaskForJsonFile(cubeName, catalogName, xmlaFile,
                            "xmla", xmlaConnectionDetails);
                    if (xmlaTask != null) cubeTasks.add(xmlaTask);
                    else logger.warning("Failed to create XMLA JSON task for cube: " + cubeName);
                } else {
                    logger.fine("XMLA queries file not found for cube: " + cubeName + " (looking for: " + xmlaFileName + ")");
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

    private MavenTaskDto<OpenStep> createTaskForJsonFile(String cubeName, String catalogName, File queryFile,
                                                        String protocol, Map<String, String> connectionDetails) {
        try {
            String queryFilePath = QUERIES_DIR + "/" + queryFile.getName();

            String sanitizedCubeNameForLogs = cubeName.replace(" ", "_");
            String sanitizedCatalogNameForLogs = catalogName.replace(" ", "_");

            String taskName = String.format("%s %s Concurrent Open Simulation (JSON)", cubeName, protocol.toUpperCase());
            MavenTaskDto<OpenStep> task = new MavenTaskDto<>(taskName);

            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);

            task.setModel(cubeName);

            task.setRunDescription(String.format("%s %s Concurrent Open Cube Tests (JSON)", cubeName, protocol.toUpperCase()));

            List<OpenStep> openSteps = new ArrayList<>();
            openSteps.add(new AtOnceUsersOpenInjectionStep(1));
            task.setInjectionSteps(openSteps);

            task.setMavenCommand("gatling:test");
            task.setRunId("ConcurrentOpenRun-" + sanitizedCatalogNameForLogs + "-" + sanitizedCubeNameForLogs + "-" + protocol + "-JSON");
            task.setRunLogFileName(sanitizedCatalogNameForLogs + "_" + sanitizedCubeNameForLogs + "_" + protocol + "_concurrent_open.log");
            task.setLoggingAsAppend(false);

            Map<String, String> additionalProperties = new HashMap<>();
            additionalProperties.put("queryFile", queryFilePath);
            additionalProperties.put("queryFileType", "json");
            additionalProperties.put("catalogName", catalogName);
            additionalProperties.put("cubeName", cubeName);
            additionalProperties.put("protocol", protocol);

            // pass connection details as provided in systems.properties
            additionalProperties.put("url", connectionDetails.getOrDefault("url", ""));
            additionalProperties.put("username", connectionDetails.getOrDefault("username", ""));
            additionalProperties.put("password", connectionDetails.getOrDefault("password", ""));

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

            logger.info("Created concurrent open step task - Cube: " + cubeName +
                    ", Catalog: " + catalogName +
                    ", Protocol: " + protocol +
                    ", Query file: " + queryFile.getName());

            return task;

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating JSON task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }

    private MavenTaskDto<OpenStep> createTaskForCsvFile(String cubeName, String catalogName, File csvFile,
                                                       String protocol, Map<String, String> connectionDetails,
                                                       boolean hasHeader) {
        try {
            String sanitizedCubeNameForLogs = cubeName.replace(" ", "_");
            String sanitizedCatalogNameForLogs = catalogName.replace(" ", "_");

            String taskName = String.format("%s %s Concurrent Open Simulation (CSV)", cubeName, protocol.toUpperCase());
            MavenTaskDto<OpenStep> task = new MavenTaskDto<>(taskName);

            String simulationClass;
            if ("xmla".equals(protocol)) {
                simulationClass = "com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation";
            } else {
                simulationClass = "com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation";
            }
            task.setSimulationClass(simulationClass);

            task.setModel(cubeName);

            task.setRunDescription(String.format("%s %s Concurrent Open Cube Tests (CSV)", cubeName, protocol.toUpperCase()));

            List<OpenStep> openSteps = new ArrayList<>();
            openSteps.add(new AtOnceUsersOpenInjectionStep(1));
            task.setInjectionSteps(openSteps);

            task.setMavenCommand("gatling:test");
            task.setRunId("ConcurrentOpenRun-" + sanitizedCatalogNameForLogs + "-" + sanitizedCubeNameForLogs + "-" + protocol + "-CSV");
            task.setRunLogFileName(sanitizedCatalogNameForLogs + "_" + sanitizedCubeNameForLogs + "_" + protocol + "_concurrent_open_csv.log");
            task.setLoggingAsAppend(false);

            Map<String, String> additionalProperties = new HashMap<>();
            additionalProperties.put("queryFile", INGEST_DIR + "/" + csvFile.getName());
            additionalProperties.put("queryFileType", "csv");
            additionalProperties.put("csvHasHeader", String.valueOf(hasHeader));
            additionalProperties.put("catalogName", catalogName);
            additionalProperties.put("cubeName", cubeName);
            additionalProperties.put("protocol", protocol);

            additionalProperties.put("url", connectionDetails.getOrDefault("url", ""));
            additionalProperties.put("username", connectionDetails.getOrDefault("username", ""));
            additionalProperties.put("password", connectionDetails.getOrDefault("password", ""));

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

            logger.info("Created concurrent open step CSV task - Cube: " + cubeName +
                    ", Catalog: " + catalogName +
                    ", Protocol: " + protocol +
                    ", CSV file: " + csvFile.getName() +
                    ", hasHeader: " + hasHeader);

            return task;

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating CSV task for cube: " + cubeName + ", protocol: " + protocol, e);
            return null;
        }
    }

    private static class CubeConfig {
        final String cubeName;
        final String catalogName;

        CubeConfig(String cubeName, String catalogName) {
            this.cubeName = cubeName;
            this.catalogName = catalogName;
        }
    }
}