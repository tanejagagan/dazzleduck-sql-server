package io.dazzleduck.sql.search;

import org.apache.lucene.analysis.Analyzer;

public interface Indexer {

    /**
     *
     * @param analyzer Analyzer used for tokenization
     * @param sources Source files which need to be indexed
     * @param indexFile File which will have index data
     * read the data from source files and write it to indexFile
     */
    void create(Analyzer analyzer,
                String[] sources,
                String indexFile);
}
