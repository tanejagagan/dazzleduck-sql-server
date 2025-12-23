package io.dazzleduck.sql.flight.stream;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

public class FlightStreamReader extends ArrowReader {
    private final FlightStream flightStream;

    public static FlightStreamReader of(FlightStream flightStream, BufferAllocator bufferAllocator) {
        return new FlightStreamReader(flightStream, bufferAllocator);
    }

    protected FlightStreamReader(FlightStream flightStream, BufferAllocator bufferAllocator) {
        super(bufferAllocator);
        this.flightStream = flightStream;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        return flightStream.next();
    }


    /**
     * Returns the number of bytes read from the stream.
     *
     * <p>Note: FlightStream does not provide byte-level metrics, so this method
     * always returns 0. This is a known limitation when wrapping FlightStream
     * as an ArrowReader.
     *
     * @return 0 (byte tracking not supported for FlightStream)
     */
    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            flightStream.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close FlightStream", e);
        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return flightStream.getSchema();
    }

    @Override
    public  VectorSchemaRoot getVectorSchemaRoot(){
        return flightStream.getRoot();
    }
}
