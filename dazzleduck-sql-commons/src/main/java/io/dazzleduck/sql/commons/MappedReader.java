package io.dazzleduck.sql.commons;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MappedReader extends ArrowReader {

    private final Field newField;
    private final Function function;
    private final ArrowReader internal;
    private final Schema schema;
    private final List<String> inputColumns;

    public MappedReader(BufferAllocator bufferAllocator,
                        ArrowReader reader,
                        Function function,
                        List<String> inputColumns,
                        Field newField) throws IOException {
        super(bufferAllocator);
        this.function = function;
        this.internal = reader;
        this.inputColumns = new ArrayList<>(inputColumns);
        this.newField = newField;
        List<Field> fieldList = new ArrayList<>(internal.getVectorSchemaRoot().getSchema().getFields());
        fieldList.add(newField);
        this.schema = new Schema(fieldList);
    }

    public static void copy(VectorSchemaRoot originalRoot,
                            VectorSchemaRoot targetRoot) {
        List<TransferPair> transferPairs = new ArrayList<>();
        for (FieldVector vector : originalRoot.getFieldVectors()) {
            TransferPair transferPair = vector.makeTransferPair(targetRoot.getVector(vector.getName()));
            transferPairs.add(transferPair);
        }
        for (TransferPair transferPair : transferPairs) {
            transferPair.transfer();
        }
        targetRoot.setRowCount(originalRoot.getRowCount());
        originalRoot.close();
    }


    @Override
    public boolean loadNextBatch() throws IOException {
        if(internal.loadNextBatch()) {
            VectorSchemaRoot internalRoot = internal.getVectorSchemaRoot();
            VectorSchemaRoot root = getVectorSchemaRoot();
            copy(internalRoot, root);
            List<FieldVector> inputs = inputColumns.stream().map(root::getVector).toList();
            FieldVector vsrVector = root.getVector(newField);
            vsrVector.allocateNew();
            vsrVector.setValueCount(root.getRowCount());
            function.apply(inputs, vsrVector);
            return true;
        }
        return false;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {

    }

    @Override
    protected Schema readSchema() throws IOException {
        return schema;
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
    }


    public static interface Function {
        void apply(List<FieldVector> source, FieldVector target);
    }
}