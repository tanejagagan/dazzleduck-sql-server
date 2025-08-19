package io.dazzleduck.sql.flight.server.auth2;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class AdvanceBasicCallHeaderAuthenticator implements CallHeaderAuthenticator {


    private static final Logger logger = LoggerFactory.getLogger(AdvanceBasicCallHeaderAuthenticator.class);
    private final AdvanceCredentialValidator validator;

    private final List<String> claimHeaders;

    public AdvanceBasicCallHeaderAuthenticator(AdvanceCredentialValidator validator, List<String> claimHeaders) {
        this.validator = validator;
        this.claimHeaders = claimHeaders;
    }
        @Override
        public AuthResult authenticate(CallHeaders incomingHeaders) {
            try {
                final String authEncoded =
                        AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BASIC_PREFIX);
                if (authEncoded == null) {
                    throw CallStatus.UNAUTHENTICATED.toRuntimeException();
                }
                // The value has the format Base64(<username>:<password>)
                final String authDecoded =
                        new String(Base64.getDecoder().decode(authEncoded), StandardCharsets.UTF_8);
                final int colonPos = authDecoded.indexOf(':');
                if (colonPos == -1) {
                    throw CallStatus.UNAUTHENTICATED.toRuntimeException();
                }

                final String user = authDecoded.substring(0, colonPos);
                final String password = authDecoded.substring(colonPos + 1);
                return validator.validate(user, password, claimHeaders, incomingHeaders);
            } catch (UnsupportedEncodingException ex) {
                // Note: Intentionally discarding the exception cause when reporting back to the client for
                // security purposes.
                logger.error("Authentication failed due to missing encoding.", ex);
                throw CallStatus.INTERNAL.toRuntimeException();
            } catch (FlightRuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                // Note: Intentionally discarding the exception cause when reporting back to the client for
                // security purposes.
                logger.error("Authentication failed.", ex);
                throw CallStatus.UNAUTHENTICATED.toRuntimeException();
            }
        }


    public interface AdvanceCredentialValidator {
        /**
         * Validate the supplied credentials (username/password) and return the peer identity.
         *
         * @param username The username to validate.
         * @param password The password to validate.
         * @param callHeaders The header coming in the request
         * @return The peer identity if the supplied credentials are valid.
         * @throws Exception If the supplied credentials are not valid.
         */
        AuthResult validate(String username, String password, List<String> claimHeaders, CallHeaders callHeaders) throws Exception;
    }
}
