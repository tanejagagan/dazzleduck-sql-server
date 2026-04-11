package io.dazzleduck.sql.common;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.http.HttpClient;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public final class SslUtils {

    private SslUtils() {}

    /**
     * Creates an SSLContext that accepts any certificate without validation.
     * Hostname verification is also disabled via the returned SSLParameters.
     */
    public static SSLContext trustAllSslContext() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[]{new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }}, new SecureRandom());
            return ctx;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException("Failed to create trust-all SSLContext", e);
        }
    }

    /**
     * Creates an HttpClient that accepts any certificate and skips hostname verification.
     */
    public static HttpClient trustAllHttpClient() {
        SSLParameters sslParameters = new SSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("");
        return HttpClient.newBuilder()
                .sslContext(trustAllSslContext())
                .sslParameters(sslParameters)
                .build();
    }
}
