package com.atscale.java.jdbc;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.galaxio.gatling.javaapi.JdbcDsl.DB;
import org.galaxio.gatling.javaapi.protocol.JdbcProtocolBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.utils.PropertiesManager;
import com.zaxxer.hikari.HikariConfig;

@SuppressWarnings("unused")
public class JdbcProtocol {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcProtocol.class);
    
    /**
     * Creates a JdbcProtocolBuilder for the AtScale JDBC connection.
     *
     * @return JdbcProtocolBuilder configured with AtScale JDBC connection details.
     */
    public static JdbcProtocolBuilder forDatabase(String model) {
        String url = getJdbcUrl(model);
        String userName = PropertiesManager.getAtScaleJdbcUserName(model);
        String password = PropertiesManager.getAtScaleJdbcPassword(model);
        int maxPool = PropertiesManager.getAtScaleJdbcMaxPoolSize(model);
        String useAggregates = PropertiesManager.getJdbcUseAggregates();
        String useLocalCache = PropertiesManager.getJdbcUseLocalCache();
        String createAggregates = PropertiesManager.getJdbcGenerateAggregates();
        String initSql = String.format("set use_local_cache = %s; set create_aggregates = %s; set use_aggregates = %s", 
                                      useLocalCache, createAggregates, useAggregates);

        LOGGER.info("JDBC URL: {}", url);
        LOGGER.info("JDBC Username: {}", userName);
        LOGGER.info("JDBC Max Pool Size: {}", maxPool);
        LOGGER.info("Initializing each connection with: {}", initSql);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(maxPool);
        hikariConfig.setConnectionInitSql(initSql);
        hikariConfig.setConnectionTestQuery("SELECT 1");
        
        // Add SSL properties to connection properties
        Properties dataSourceProperties = new Properties();
        dataSourceProperties.setProperty("ssl", "true");
        dataSourceProperties.setProperty("trustStore", "./cacerts");
        dataSourceProperties.setProperty("trustStorePassword", "changeit");
        hikariConfig.setDataSourceProperties(dataSourceProperties);
        
        // Additional connection settings for better reliability
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);
        hikariConfig.setValidationTimeout(5000);
        
        LOGGER.debug("HikariConfig configured for model: {}", model);
        
        return DB().hikariConfig(hikariConfig);
    }

    private static String getJdbcUrl(String model) {
        String url = PropertiesManager.getAtScaleJdbcConnection(model);
        // Split the URL to encode only the database name for Hive
        if(url.toLowerCase().contains("hive")) {
            int idx = url.indexOf('/', "jdbc:hive2://".length());
            if (idx > 0 && idx + 1 < url.length()) {
                String base = url.substring(0, idx + 1);
                String dbName = url.substring(idx + 1);
                dbName = URLEncoder.encode(dbName, StandardCharsets.UTF_8).replace("+", "%20");
                url = base + dbName;
            }
        }
        
        // Add SSL parameters to URL if not already present
        if (!url.contains("ssl=")) {
            if (url.contains("?")) {
                url += "&ssl=true&trustStore=./cacerts&trustStorePassword=changeit";
            } else {
                url += "?ssl=true&trustStore=./cacerts&trustStorePassword=changeit";
            }
        }
        
        LOGGER.debug("Final JDBC URL: {}", url);
        return url;
    }
}