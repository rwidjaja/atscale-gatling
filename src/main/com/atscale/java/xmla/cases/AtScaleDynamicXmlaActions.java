package com.atscale.java.xmla.cases;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.dao.QueryHistoryDto;
import com.atscale.java.utils.CsvLoaderUtil;
import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.utils.QueryHistoryFileUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.core.CoreDsl.bodyString;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;
import io.gatling.javaapi.http.HttpRequestActionBuilder;

@SuppressWarnings("unused")
public class AtScaleDynamicXmlaActions {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleDynamicXmlaActions.class);
    private static final String WORKING_DIR = "working_dir";
    private static final String CONFIG_DIR = "config";
    private static final String RUNTIME_FILE = "runtime.json";
    private static final int DEFAULT_TIMEOUT_SECONDS = 120;
    
    // Cache for timeout configuration
    private static Integer cachedXmlaRequestTimeoutSeconds = null;
    private static long lastConfigModified = 0;

    public AtScaleDynamicXmlaActions() {
        super();
    }

    /**
     * Get XMLA request timeout from runtime.json configuration
     * Follows the same pattern as ClosedStepConcurrentSimulationExecutor
     */
    public static int getXmlaRequestTimeoutSeconds() {
        File runtimeFile = new File(WORKING_DIR, CONFIG_DIR + File.separator + RUNTIME_FILE);
        
        // Check if we need to reload (file changed or not cached)
        if (cachedXmlaRequestTimeoutSeconds != null && runtimeFile.exists() && 
            runtimeFile.lastModified() <= lastConfigModified) {
            return cachedXmlaRequestTimeoutSeconds;
        }
        
        // Load configuration
        cachedXmlaRequestTimeoutSeconds = loadXmlaRequestTimeoutFromConfig();
        if (runtimeFile.exists()) {
            lastConfigModified = runtimeFile.lastModified();
        }
        
        return cachedXmlaRequestTimeoutSeconds;
    }

    private static int loadXmlaRequestTimeoutFromConfig() {
        File runtimeFile = new File(WORKING_DIR, CONFIG_DIR + File.separator + RUNTIME_FILE);
        
        // Check if runtime.json exists
        if (!runtimeFile.exists()) {
            LOGGER.warn("runtime.json not found at {} using default XMLA request timeout: {} seconds", 
                       runtimeFile.getAbsolutePath(), DEFAULT_TIMEOUT_SECONDS);
            return DEFAULT_TIMEOUT_SECONDS;
        }
        
        try (FileInputStream fis = new FileInputStream(runtimeFile);
             InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8)) {
            
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(isr);
            
            // Get the xmlaRequestTimeoutSeconds from the root of the JSON
            if (rootNode.has("xmlaRequestTimeoutSeconds")) {
                int timeout = rootNode.get("xmlaRequestTimeoutSeconds").asInt(DEFAULT_TIMEOUT_SECONDS);
                LOGGER.info("Loaded XMLA request timeout from runtime.json: {} seconds", timeout);
                return timeout;
            } else {
                LOGGER.warn("xmlaRequestTimeoutSeconds not found in runtime.json, using default: {} seconds", 
                           DEFAULT_TIMEOUT_SECONDS);
                return DEFAULT_TIMEOUT_SECONDS;
            }
            
        } catch (Exception e) {
            LOGGER.error("Error reading timeout from runtime.json, using default: {} seconds", 
                        DEFAULT_TIMEOUT_SECONDS, e);
            return DEFAULT_TIMEOUT_SECONDS;
        }
    }

    private List<NamedHttpRequestActionBuilder> createXmlaPayloads(List<QueryHistoryDto> history, String cubeName, String catalog) {
        List<NamedHttpRequestActionBuilder> builders = new ArrayList<>();
        for (QueryHistoryDto query : history) {
                String queryName = query.getQueryName();
                String inboundTextAsMd5Hash = query.getInboundTextAsMd5Hash();
                String body = injectXmlaQuery(query.getInboundText(), cubeName, catalog);
                builders.add(new NamedHttpRequestActionBuilder(httpRequest(queryName, body), queryName, inboundTextAsMd5Hash, body));
                LOGGER.debug("Created XMLA payload for query: {} hash: {} and body {}", queryName, query.getInboundTextAsMd5Hash(), body);
            }
            return builders;
    }

    public NamedHttpRequestActionBuilder[] createPayloadsIngestedXmlaQueries(String model, String cubeName, String catalog, String ingestionFileName, boolean hasHeader) {
        CsvLoaderUtil csvLoader = new CsvLoaderUtil(ingestionFileName, hasHeader);

        List<QueryHistoryDto> history = csvLoader.loadQueriesFromCsv();
        if (history.isEmpty()) {
            throw new IllegalArgumentException(String.format("No queries found in the history file: %s", csvLoader.getFilePath()));
        }

        List<NamedHttpRequestActionBuilder> builders = createXmlaPayloads(history, cubeName, catalog);
        return builders.toArray(new NamedHttpRequestActionBuilder[0]);
    }

    public NamedHttpRequestActionBuilder[] createPayloadsXmlaQueries(String model, String cubeName, String catalog) {
        String filePath = QueryHistoryFileUtil.getXmlaFilePath(model);
        try {
            List<QueryHistoryDto> history = QueryHistoryFileUtil.readQueryHistoryFromFile(filePath);
            if (history.isEmpty()) {
                throw new IllegalArgumentException(String.format("No queries found in the history file: %s", filePath));
            }

            List<NamedHttpRequestActionBuilder> builders = createXmlaPayloads(history, cubeName, catalog);
            return builders.toArray(new NamedHttpRequestActionBuilder[0]);
        } catch(FileNotFoundException e) {
            throw new RuntimeException(String.format("Query history file not found: %s.  It should be generated by running the QueryExtractExecutor. See README.md for instructions", filePath), e);
        } catch(IOException e) {
            throw new RuntimeException("Error reading query history file: " + filePath, e);
        }
    }

    private HttpRequestActionBuilder httpRequest(String queryName, String body) {
        int timeoutSeconds = getXmlaRequestTimeoutSeconds();
        
        LOGGER.debug("Setting request timeout for query '{}' to {} seconds", queryName, timeoutSeconds);
        
        return http(queryName)
                .post("")
                .body(StringBody(body)).asXml()
                .requestTimeout(Duration.ofSeconds(timeoutSeconds))
                .check(
                        status().saveAs("responseStatus"),
                        status().is(200),
                        bodyString().saveAs("responseBody")
                );
    }

    private String injectXmlaQuery(String queryBody, String cube, String catalog) {
        queryBody = org.apache.commons.text.StringEscapeUtils.escapeXml11(queryBody);
        return String.format("""
                <Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
                    <Body>
                        <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
                            <Command>
                                <Statement>%s</Statement>
                            </Command>
                            <Properties>
                                <PropertyList>
                                    <Cube>%s</Cube>
                                    <Catalog>%s</Catalog>
                                    <UseAggregates>%s</UseAggregates>
                                    <GenerateAggregates>%s</GenerateAggregates>
                                    <UseQueryCache>%s</UseQueryCache>
                                    <UseAggregateCache>%s</UseAggregateCache>
                                </PropertyList>
                            </Properties>
                        </Execute>
                    </Body>
                </Envelope>
                """, queryBody, cube, catalog, PropertiesManager.getXmlaUseAggregates(),
                PropertiesManager.getXmlaGenerateAggregates(),
                PropertiesManager.getXmlaUseQueryCache(),
                PropertiesManager.getXmlaUseAggregateCache());
    }
}