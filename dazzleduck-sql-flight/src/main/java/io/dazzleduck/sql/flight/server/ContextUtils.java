package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import org.apache.arrow.flight.*;

import java.util.Map;
import java.util.UnknownFormatConversionException;

public interface ContextUtils {
    static <T> T getValue(FlightProducer.CallContext context, String key, T defaultValue, Class<T> tClass) {
        var header =  context.getMiddleware(FlightConstants.HEADER_KEY);
        var fromHeaderString  = header.headers().get(key);
        if(fromHeaderString == null) {
            return defaultValue;
        }
        var fn = Headers.EXTRACTOR.get(tClass);
        if(fn == null) {
            throw new UnknownFormatConversionException(tClass.getName());
        }
        return (T) fn.apply(fromHeaderString);
    }
}
