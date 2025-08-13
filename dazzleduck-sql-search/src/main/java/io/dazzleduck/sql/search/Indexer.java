package io.dazzleduck.sql.search;

import org.apache.lucene.analysis.Analyzer;

public interface Indexer {
    /**
     *
     * @param analyzer Analyzer to be used
     * @param sources Source of the data
     * @param indexFile
     * Iterates over the sources and build the index file based on the specification
     * The  partitioning of index files should match with the partitioning of sources
     */

    void create(Analyzer analyzer,
                String[] sources,
                String indexFile);
}
