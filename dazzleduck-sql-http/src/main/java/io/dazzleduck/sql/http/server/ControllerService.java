package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

public interface ControllerService {
    static FlightProducer.CallContext createContext(ServerRequest request) {
        var subjectAndVerifiedClaims = request.context().get(JwtAuthenticationFilter.SUBJECT_KEY, SubjectAndVerifiedClaims.class);
        return new SyntheticFlightContext(request.headers().toMap(),
                subjectAndVerifiedClaims.orElse(null), request.query().toMap());
    }

    static void sendFlightError(ServerResponse response, Throwable cause) {
        String errorMsg = cause.getMessage() != null ? cause.getMessage() : "Internal server error";
        Status httpStatus = Status.INTERNAL_SERVER_ERROR_500;
        if (cause instanceof FlightRuntimeException flightEx) {
            if (flightEx.status().description() != null) errorMsg = flightEx.status().description();
            httpStatus = flightStatusToHttp(flightEx.status().code());
        }
        response.status(httpStatus).send(errorMsg);
    }

    static Status flightStatusToHttp(FlightStatusCode code) {
        return switch (code) {
            case OK              -> Status.OK_200;
            case INVALID_ARGUMENT -> Status.BAD_REQUEST_400;
            case UNAUTHENTICATED -> Status.UNAUTHORIZED_401;
            case UNAUTHORIZED    -> Status.FORBIDDEN_403;
            case NOT_FOUND       -> Status.NOT_FOUND_404;
            case ALREADY_EXISTS  -> Status.CONFLICT_409;
            case TIMED_OUT       -> Status.GATEWAY_TIMEOUT_504;
            case UNIMPLEMENTED   -> Status.NOT_IMPLEMENTED_501;
            case UNAVAILABLE     -> Status.SERVICE_UNAVAILABLE_503;
            default              -> Status.INTERNAL_SERVER_ERROR_500;
        };
    }
}
