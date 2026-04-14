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

    /**
     * When this environment variable is set (to any non-empty value), all HTTP clients
     * will skip certificate validation and hostname verification. Intended for development
     * and testing environments with self-signed certificates.
     */
    public static final String TRUST_SELF_SIGNED_CERTS_ENV = "DD_TRUST_SELF_SIGNED_CERTS";

    private static final boolean TRUST_SELF_SIGNED = System.getenv(TRUST_SELF_SIGNED_CERTS_ENV) != null
            && !System.getenv(TRUST_SELF_SIGNED_CERTS_ENV).isEmpty();

    private SslUtils() {
    }

    /**
     * Returns an SSLContext appropriate for the current environment.
     * If {@code DD_TRUST_SELF_SIGNED_CERTS} is set, returns a trust-all context
     * (no certificate or hostname validation). Otherwise, returns the JVM default context.
     */
    public static SSLContext sslContext() {
        if (TRUST_SELF_SIGNED) {
            return trustAllSslContext();
        }
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get default SSLContext", e);
        }
    }

    /**
     * Returns an HttpClient appropriate for the current environment.
     * If {@code DD_TRUST_SELF_SIGNED_CERTS} is set, returns a client that skips
     * certificate and hostname validation. Otherwise, returns a default HttpClient.
     */
    public static HttpClient httpClient() {
        if (TRUST_SELF_SIGNED) {
            return trustAllHttpClient();
        }
        return HttpClient.newHttpClient();
    }

    /**
     * Creates an SSLContext that accepts any certificate without validation.
     * Hostname verification is also disabled via the returned SSLParameters.
     */
    public static SSLContext trustAllSslContext() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[]{new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
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
