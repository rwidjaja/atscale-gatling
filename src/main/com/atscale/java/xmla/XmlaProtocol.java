package com.atscale.java.xmla;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Base64;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atscale.java.utils.PropertiesManager;

import static io.gatling.javaapi.http.HttpDsl.http;
import io.gatling.javaapi.http.HttpProtocolBuilder;

@SuppressWarnings("unused")
public class XmlaProtocol {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlaProtocol.class);

    // Initialize SSL to trust all certificates (for testing)
    static {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                }
            }, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize SSL context: {}", e.getMessage(), e);
        }
    }

    public static HttpProtocolBuilder forXmla(String model) {
        String url = PropertiesManager.getAtScaleXmlaConnection(model);
        Integer maxConnections = PropertiesManager.getAtScaleXmlaMaxConnectionsPerHost();

        if (PropertiesManager.isContainerVersion(model)) {
            LOGGER.info("Configured for container version. Auth token is part of the URL.");
            LOGGER.info("Configured for max connections per host: {}", maxConnections);
            return http.baseUrl(url)
                    .contentTypeHeader("text/xml; charset=UTF-8")
                    .acceptHeader("text/xml")
                    .maxConnectionsPerHost(maxConnections);
        } else {
            LOGGER.info("Configured for installer version. Will obtain bearer auth token.");
            LOGGER.info("Configured for max connections per host: {}", maxConnections);
            String authUrl = PropertiesManager.getAtScaleXmlaAuthConnection(model);
            String tokenUserName = PropertiesManager.getAtScaleXmlaAuthUserName(model);
            String tokenPassword = PropertiesManager.getAtScaleXmlaAuthPassword(model);
            String bearerToken = getBearerToken(authUrl, tokenUserName, tokenPassword);
            LOGGER.info("Obtained bearer token for user {} and model {}.", tokenUserName, model);

            return http.baseUrl(url)
                    .contentTypeHeader("text/xml; charset=UTF-8")
                    .acceptHeader("text/xml")
                    .authorizationHeader(bearerToken)
                    .maxConnectionsPerHost(maxConnections);
        }
    }

    public static String getBearerToken(String urlString, String username, String password) {
        LOGGER.info("Getting bearer token from URL: {}", urlString);
        try {
            URL url = new URL(urlString);
            HttpURLConnection conn;
            
            if (urlString.startsWith("https")) {
                conn = (HttpsURLConnection) url.openConnection();
            } else {
                conn = (HttpURLConnection) url.openConnection();
            }
            
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
            conn.setRequestProperty("Accept", "application/json");

            int responseCode = conn.getResponseCode();
            LOGGER.info("Token request response code: {}", responseCode);
            
            if (responseCode != 200) {
                String errorMessage = readErrorStream(conn);
                LOGGER.error("Failed to get bearer token. Response code: {}, Error: {}", responseCode, errorMessage);
                throw new RuntimeException("Failed to get bearer token. HTTP error code: " + responseCode + ", Error: " + errorMessage);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                String tokenResponse = response.toString().trim();
                LOGGER.debug("Raw token response: {}", tokenResponse);
                
                // Try to parse JSON response if it looks like JSON
                if (tokenResponse.startsWith("{") && tokenResponse.endsWith("}")) {
                    try {
                        // Simple JSON parsing for token extraction
                        if (tokenResponse.contains("\"token\"")) {
                            int tokenStart = tokenResponse.indexOf("\"token\":\"") + 9;
                            int tokenEnd = tokenResponse.indexOf("\"", tokenStart);
                            tokenResponse = tokenResponse.substring(tokenStart, tokenEnd);
                        } else if (tokenResponse.contains("\"access_token\"")) {
                            int tokenStart = tokenResponse.indexOf("\"access_token\":\"") + 16;
                            int tokenEnd = tokenResponse.indexOf("\"", tokenStart);
                            tokenResponse = tokenResponse.substring(tokenStart, tokenEnd);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Could not parse token from JSON response, using raw response");
                    }
                }
                
                LOGGER.info("Successfully obtained bearer token");
                return String.format("Bearer %s", tokenResponse);
            }
        } catch (IOException e) {
            LOGGER.error("Error while getting bearer token for {} from {}: {}", username, urlString, e.getMessage(), e);
            throw new RuntimeException("Error while getting bearer token: " + e.getMessage(), e);
        }
    }
    
    private static String readErrorStream(HttpURLConnection conn) {
        try {
            if (conn.getErrorStream() != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        errorResponse.append(line);
                    }
                    return errorResponse.toString();
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error reading error stream: {}", e.getMessage());
        }
        return "No error details available";
    }
}