package com.atscale.java.xmla.scenarios;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.utils.PropertiesManager;
import com.atscale.java.xmla.cases.AtScaleDynamicXmlaActions;
import com.atscale.java.xmla.cases.NamedHttpRequestActionBuilder;

import io.gatling.javaapi.core.ChainBuilder;
import static io.gatling.javaapi.core.CoreDsl.exec;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import io.gatling.javaapi.core.ScenarioBuilder;

public class AtScaleXmlaScenario {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaScenario.class);
    private static final Logger SESSION_LOGGER = LoggerFactory.getLogger("XmlaLogger");

    public AtScaleXmlaScenario() {
        super();
    }

    /**
     * Builds a Gatling scenario that executes a series of dynamic JDBC queries.
     * The scenario is constructed using the actions defined in AtScaleDynamicJdbcActions.
     *
     * @return A ScenarioBuilder instance representing the dynamic query execution scenario.
     */
    public ScenarioBuilder buildScenario(String model, String cube, String catalog, String gatlingRunId, String ingestionFile, boolean ingestionFileHasHeader) {
        NamedHttpRequestActionBuilder[] builders;
        boolean logResponseBody = PropertiesManager.getLogXmlaResponseBody(model);
        Long throttleBy = PropertiesManager.getAtScaleThrottleMs();
        AtScaleDynamicXmlaActions xmlaActions = new AtScaleDynamicXmlaActions();

        if(StringUtils.isNotEmpty(ingestionFile)) {
            builders = xmlaActions.createPayloadsIngestedXmlaQueries(model, cube, catalog, ingestionFile, ingestionFileHasHeader);
        } else {
            builders = xmlaActions.createPayloadsXmlaQueries(model, cube, catalog);
        }

        List<ChainBuilder> chains = Arrays.stream(builders)
                .map(namedBuilder ->
                        exec(session -> {
                            // Initialize all variables to prevent nulls
                            return session
                                    .set("queryStart", System.currentTimeMillis())
                                    .set("errorMessage", "")
                                    .set("responseBody", "")
                                    .set("responseStatus", 0);
                        })
                        .tryMax(2, "retryCount").on( // Retry failed requests
                            exec(namedBuilder.builder)
                            .exec(session -> {
                                long end = System.currentTimeMillis();
                                long start = session.getLong("queryStart");
                                long duration = end - start;
                                
                                // Safely get values with defaults
                                String response = safeGetString(session, "responseBody", "");
                                Integer statusCode = safeGetInt(session, "responseStatus", 0);
                                String errorMsg = safeGetString(session, "errorMessage", "");
                                
                                // Determine success
                                boolean isSuccess = !session.isFailed() && statusCode >= 200 && statusCode < 300;
                                String status = isSuccess ? "SUCCEEDED" : "FAILED";
                                
                                // If failed but no error message, create one
                                if (!isSuccess && errorMsg.isEmpty()) {
                                    if (statusCode == 0) {
                                        errorMsg = "Connection failed or timeout";
                                    } else {
                                        errorMsg = "HTTP " + statusCode;
                                    }
                                    session = session.set("errorMessage", errorMsg);
                                }
                                
                                int responseSize = response.length();
                                String runId = gatlingRunId != null ? gatlingRunId : "unknown";
                                
                                // Log based on configuration
                                if (logResponseBody) {
                                    SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' inboundTextAsMd5Hash='{}' start={} end={} duration={} responseSize={} response={}",
                                            runId, status, session.userId(), model, cube, catalog, 
                                            namedBuilder.queryName, namedBuilder.inboundTextAsMd5Hash, 
                                            start, end, duration, responseSize, response);
                                } else {
                                    SESSION_LOGGER.info("xmlaLog gatlingRunId='{}' status='{}' gatlingSessionId={} model='{}' cube='{}' catalog='{}' queryName='{}' inboundTextAsMd5Hash='{}' start={} end={} duration={} responseSize={}",
                                            runId, status, session.userId(), model, cube, catalog, 
                                            namedBuilder.queryName, namedBuilder.inboundTextAsMd5Hash, 
                                            start, end, duration, responseSize);
                                }
                                
                                // Log error if any
                                if (!isSuccess) {
                                    LOGGER.warn("Query '{}' with hash '{}' failed: status={}, error={}", 
                                            namedBuilder.queryName, namedBuilder.inboundTextAsMd5Hash, 
                                            statusCode, errorMsg);
                                }
                                
                                return session.set("lastStatus", status);
                            })
                        )
                        .exec(session -> {
                            // If we're here after retries and still failed, ensure we have an error message
                            String lastStatus = safeGetString(session, "lastStatus", "FAILED");
                            if ("FAILED".equals(lastStatus)) {
                                String errorMsg = safeGetString(session, "errorMessage", "");
                                if (errorMsg.isEmpty()) {
                                    errorMsg = "Max retries exceeded";
                                    session = session.set("errorMessage", errorMsg);
                                }
                                // Ensure Gatling has a non-null error message
                                session = session.set("gatlingErrorMessage", errorMsg);
                            }
                            return session;
                        })
                        .pause(Duration.ofMillis(throttleBy)))
                .collect(Collectors.toList());
        
        return scenario("AtScale XMLA Scenario").exec(chains).pause(Duration.ofMillis(10));
    }
    
    private String safeGetString(io.gatling.javaapi.core.Session session, String key, String defaultValue) {
        try {
            String value = session.getString(key);
            return value != null ? value : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    private Integer safeGetInt(io.gatling.javaapi.core.Session session, String key, Integer defaultValue) {
        try {
            return session.getInt(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}