package com.atscale.java.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class PropertiesManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesManager.class);
    private final Properties properties = new Properties();
    private final String propertiesFileName;
    private final String differentiator;
    private static final PropertiesManager instance = new PropertiesManager();

    private PropertiesManager() {
        // Initialize with default values first
        String fileName = "systems.properties";
        String diff = "";
        
        try {
            // Determine filename
            if (System.getProperty("systems.properties.file") != null) {
                fileName = System.getProperty("systems.properties.file");
                LOGGER.info("Loading properties file from system property: {}", fileName);
                diff = "-altprops";
            }

            // First try: Explicit override with -D
            Path externalFilePath = (System.getProperty("systems.properties.file") != null)
                    ? Paths.get(fileName)
                    : Paths.get("./working_dir/config/" + fileName);

            if (Files.exists(externalFilePath) && Files.isRegularFile(externalFilePath)) {
                LOGGER.info("Loading external properties file from path: {}", externalFilePath.toAbsolutePath());
                try (InputStream input = Files.newInputStream(externalFilePath)) {
                    properties.load(input);
                }
            } else {
                LOGGER.warn("External properties file not found at: {}", externalFilePath.toAbsolutePath());
                
                // Final fallback: classpath resource
                URL propertyFileURl = getClass().getClassLoader().getResource(fileName);
                if (propertyFileURl != null) {
                    LOGGER.info("Loading properties file from classpath: {}", propertyFileURl);
                    try (InputStream input = getClass().getClassLoader().getResourceAsStream(fileName)) {
                        properties.load(input);
                    }
                } else {
                    LOGGER.error("Properties file not found: {}", fileName);
                }
            }

        } catch (IOException e) {
            LOGGER.error("Error loading properties file: {}", e.getMessage());
        }
        
        // Assign to final fields after initialization
        this.propertiesFileName = fileName;
        this.differentiator = diff;
    }

    public static String getDifferentiator() {
        return instance.differentiator;
    }

    public static List<String> getAtScaleModels(){
        String property = instance.properties.getProperty("atscale.models");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.models is not set in properties file: " + instance.propertiesFileName);
        }
        String[] models = property.split("\\s*,\\s*"); // Splits on commas, trims spaces
        LOGGER.info("AtScale models loaded: {}", Arrays.toString(models));
        return Arrays.asList(models);
    }

    public static Long getAtScaleThrottleMs() {
        return Long.parseLong(getProperty("atscale.gatling.throttle.ms", "5"));
    }

    public static Integer getAtScaleXmlaMaxConnectionsPerHost() {
        return Integer.parseInt(getProperty("atscale.xmla.maxConnectionsPerHost", "20"));
    }

    public static String getJdbcUseAggregates() {
        String prop =  getProperty("atscale.jdbc.useAggregates", "true");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.jdbc.useAggregates: {} expected true or false", prop);
            return String.valueOf(true);
        }
    }

    public static String getJdbcGenerateAggregates() {
        String prop =  getProperty("atscale.jdbc.generateAggregates", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.jdbc.generateAggregates: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getJdbcUseLocalCache() {
        String prop =  getProperty("atscale.jdbc.useLocalCache", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.jdbc.useLocalCache: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getXmlaUseAggregates() {
        String prop =  getProperty("atscale.xmla.useAggregates", "true");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.useAggregates: {} expected true or false", prop);
            return String.valueOf(true);
        }
    }

    public static String getXmlaGenerateAggregates() {
        String prop =  getProperty("atscale.xmla.generateAggregates", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.generateAggregates: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getXmlaUseQueryCache() {
        String prop =  getProperty("atscale.xmla.useQueryCache", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.useQueryCache: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getXmlaUseAggregateCache() {
        String prop = getProperty("atscale.xmla.useAggregateCache", "true");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.useAggregateCache: {} expected true or false", prop);
            return String.valueOf(true);
        }
    }

    public static String getAtScalePostgresURL() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.url");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.url is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    public static String getAtScalePostgresUser() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.username");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.username is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    public static String getAtScalePostgresPassword() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.password");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.password is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    public static String getAtScaleJdbcConnection(String model) {
        String key = String.format("atscale.%s.jdbc.url", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleJdbcUserName(String model) {
        String key = String.format("atscale.%s.jdbc.username", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleJdbcPassword(String model) {
        String key = String.format("atscale.%s.jdbc.password", clean(model));
        return getProperty(key);
    }

    public static int getAtScaleJdbcMaxPoolSize(String model) {
        String key = String.format("atscale.%s.jdbc.maxPoolSize", clean(model));
        return Integer.parseInt(getProperty(key, "10"));
    }

    public static String getAtScaleXmlaConnection(String model) {
        String key = String.format("atscale.%s.xmla.url", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaCubeName(String model) {
        String key = String.format("atscale.%s.xmla.cube", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaCatalogName(String model) {
        String key = String.format("atscale.%s.xmla.catalog", clean(model));
        return getProperty(key);
    }

    public static boolean getLogSqlQueryRows(String model) {
        String key = String.format("atscale.%s.jdbc.log.resultset.rows", clean(model));
        return Boolean.parseBoolean(getProperty(key, "false"));
    }

    public static boolean getLogXmlaResponseBody(String model) {
        String key = String.format("atscale.%s.xmla.log.responsebody", clean(model));
        return Boolean.parseBoolean(getProperty(key, "false"));
    }

    public static boolean isInstallerVersion(String model) {
       return ! isContainerVersion(model);
    }

    public static boolean isContainerVersion(String model) {
       String xmlaConnection = getAtScaleXmlaConnection(model);
       return xmlaConnection.toLowerCase().contains("/engine/xmla");
    }

    public static String getAtScaleXmlaAuthConnection(String model) {
        String key = String.format("atscale.%s.xmla.auth.url", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaAuthUserName(String model) {
        String key = String.format("atscale.%s.xmla.auth.username", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaAuthPassword(String model) {
        String key = String.format("atscale.%s.xmla.auth.password", clean(model));
        return getProperty(key);
    }

    // Add this method to get cube configurations
    public static Map<String, Map<String, String>> getAtScaleCubeConfigurations() {
        Map<String, Map<String, String>> cubeConfigs = new HashMap<>();
        
        // Get all properties that contain cube configuration
        for (String key : instance.properties.stringPropertyNames()) {
            if (key.startsWith("atscale.") && key.contains(".xmla.cube")) {
                // Extract cube identifier (the part between "atscale." and ".xmla.cube")
                String cubeKey = key.substring("atscale.".length(), key.indexOf(".xmla.cube"));
                
                if (!cubeConfigs.containsKey(cubeKey)) {
                    cubeConfigs.put(cubeKey, new HashMap<>());
                }
                
                // Add this property to the cube's configuration
                String propName = key.substring(("atscale." + cubeKey + ".").length());
                cubeConfigs.get(cubeKey).put(propName, instance.properties.getProperty(key));
            }
            
            // Also capture JDBC properties for cubes
            if (key.startsWith("atscale.") && key.contains(".jdbc.url") && !key.contains("postgres.jdbc.url")) {
                String cubeKey = key.substring("atscale.".length(), key.indexOf(".jdbc.url"));
                
                if (!cubeConfigs.containsKey(cubeKey)) {
                    cubeConfigs.put(cubeKey, new HashMap<>());
                }
                
                String propName = key.substring(("atscale." + cubeKey + ".").length());
                cubeConfigs.get(cubeKey).put(propName, instance.properties.getProperty(key));
            }
        }
        
        LOGGER.info("Loaded cube configurations: {}", cubeConfigs.keySet());
        return cubeConfigs;
    }

    public static void setCustomProperties(java.util.Map<String, String> customProperties) {
        for (String key : customProperties.keySet()) {
            LOGGER.info("Setting custom property: {} of size {}", key, customProperties.get(key).length());
            instance.properties.setProperty(key, customProperties.get(key));
        }
    }

    public static boolean hasProperty(String key) {
        String property = instance.properties.getProperty(key);
        return StringUtils.isNotEmpty(property);
    }

    public static String getCustomProperty(String propertyName) {
        return getProperty(propertyName);
    }

    private static String getProperty(String key) {
        String property = instance.properties.getProperty(key);
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("Property " + key + " is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    @SuppressWarnings("all")
    private static String getProperty(String key, String defaultValue) {
        if(! instance.properties.containsKey(key)) {
            LOGGER.warn("Using default value for property {}: {}", key, defaultValue);
        }
        return instance.properties.getProperty(key, defaultValue);
    }

    private static String clean(String input) {
        String val = StringUtil.stripQuotes(input);
        return val.replace(" ", "_");
    }
}