package bench;

import org.duckdb.DuckDBConnection;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Sidecar-derivation benchmark: one ingested batch (local, uncompressed Arrow IPC files, as the
 * server stores them) is written as a base table plus N sidecar tables, each sidecar a SQL view
 * over the base rows. Strategies differ in what the sidecars are derived from and how the files
 * reach S3 (MinIO here).
 *
 * Usage: SidecarBench <rowsPerBatch> <inputFiles> <measuredRuns> <s3Endpoint>
 */
public class SidecarBench {

    static final String BUCKET = "bench";
    static final String BASE_TRANSFORMATION = "SELECT *, date_trunc('minute', ts) AS minute FROM __this";
    static final LinkedHashMap<String, String> SIDECARS = new LinkedHashMap<>();
    static {
        SIDECARS.put("metrics", "SELECT minute, service, severity, count(*) AS n, avg(duration_ms) AS avg_ms,"
                + " max(duration_ms) AS max_ms FROM __base GROUP BY ALL");
        SIDECARS.put("errors", "SELECT ts, service, trace_id, http_status, body FROM __base WHERE severity = 'ERROR'");
        SIDECARS.put("attr_keys", "SELECT DISTINCT service, unnest(attr_keys) AS attr_key FROM __base");
    }

    static DuckDBConnection db;
    static S3AsyncClient s3;
    static S3TransferManager tm;
    static Path work;
    static List<Path> inputs;
    static final ExecutorService pool = Executors.newCachedThreadPool();

    record Timing(long deriveMs, long uploadMs, long totalMs) {}

    interface Strategy {
        /** Writes base + sidecars for one batch under {@code prefix}; returns rows per output. */
        Timing run(String prefix, Map<String, Long> rowCounts) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        int rows = Integer.parseInt(args[0]);
        int files = Integer.parseInt(args[1]);
        int runs = Integer.parseInt(args[2]);
        String endpoint = args[3];

        work = Files.createTempDirectory("sidecar-bench");
        db = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        exec(db, "INSTALL arrow FROM community; LOAD arrow; INSTALL httpfs; LOAD httpfs;");
        exec(db, ("CREATE SECRET minio (TYPE s3, KEY_ID 'bench', SECRET 'benchbench', ENDPOINT '%s',"
                + " URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')").formatted(endpoint));

        s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://" + endpoint))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("bench", "benchbench")))
                .multipartEnabled(true)
                .build();
        tm = S3TransferManager.builder().s3Client(s3).build();
        try {
            s3.createBucket(b -> b.bucket(BUCKET)).join();
        } catch (CompletionException ignored) {
            // exists
        }

        inputs = generateInput(rows, files);
        long inputBytes = 0;
        for (Path p : inputs) inputBytes += Files.size(p);
        System.out.printf("input: %,d rows in %d Arrow IPC files, %,.1f MB uncompressed; DuckDB threads=%s%n",
                rows, files, inputBytes / 1e6, scalar(db, "SELECT current_setting('threads')"));

        LinkedHashMap<String, Strategy> strategies = new LinkedHashMap<>();
        strategies.put("A raw -> S3 direct (sequential COPYs)", SidecarBench::rawDirect);
        strategies.put("B raw -> S3 direct (parallel COPYs)", SidecarBench::rawDirectParallel);
        strategies.put("C temp table -> S3 direct", SidecarBench::tempTableDirect);
        strategies.put("D base -> S3, sidecars read back from S3", SidecarBench::s3Readback);
        strategies.put("E raw -> local, then parallel upload", SidecarBench::rawLocalThenUpload);
        strategies.put("F base -> local parquet, sidecars from it, then upload", SidecarBench::localParquetThenUpload);
        strategies.put("G base -> local parquet, upload overlapped with sidecars", SidecarBench::localParquetPipelined);

        Map<String, List<Timing>> results = new LinkedHashMap<>();
        Map<String, Map<String, Long>> counts = new LinkedHashMap<>();
        for (int iter = -1; iter < runs; iter++) { // iteration -1 is warm-up
            // Rotate the order each iteration so no strategy always runs first or last.
            List<String> names = new ArrayList<>(strategies.keySet());
            Collections.rotate(names, Math.max(iter, 0));
            for (String name : names) {
                String prefix = "r" + System.nanoTime();
                Map<String, Long> rc = new TreeMap<>();
                Timing t = strategies.get(name).run(prefix, rc);
                if (iter >= 0) results.computeIfAbsent(name, k -> new ArrayList<>()).add(t);
                counts.putIfAbsent(name, rc);
                if (iter == 0) verifyOnS3(name, prefix, rc);
                cleanup(prefix);
            }
        }

        // Every strategy must produce the same rows per output.
        Set<Map<String, Long>> distinct = new HashSet<>(counts.values());
        System.out.println("row counts per output: " + counts.values().iterator().next()
                + (distinct.size() == 1 ? "  (identical across strategies)" : "  MISMATCH: " + counts));

        System.out.printf("%n%-58s %10s %10s %10s %10s%n", "strategy (median of " + runs + ")", "total ms", "min ms",
                "derive ms", "upload ms");
        for (var e : results.entrySet()) {
            List<Timing> ts = e.getValue();
            System.out.printf("%-58s %10d %10d %10s %10s%n", e.getKey(),
                    median(ts.stream().mapToLong(Timing::totalMs)),
                    ts.stream().mapToLong(Timing::totalMs).min().orElse(0),
                    ts.get(0).deriveMs() < 0 ? "-" : String.valueOf(median(ts.stream().mapToLong(Timing::deriveMs))),
                    ts.get(0).uploadMs() < 0 ? "-" : String.valueOf(median(ts.stream().mapToLong(Timing::uploadMs))));
        }
        tm.close();
        s3.close();
        pool.shutdownNow();
        db.close();
    }

    // ---- strategies ------------------------------------------------------------------------

    /** A: every output is its own COPY over the raw Arrow files, written straight to S3. */
    static Timing rawDirect(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        rc.put("base", copy(db, baseOver(raw()), s3Path(prefix, "base")));
        for (var sc : SIDECARS.entrySet()) {
            rc.put(sc.getKey(), copy(db, sidecarOver(baseOver(raw()), sc.getValue()), s3Path(prefix, sc.getKey())));
        }
        return new Timing(-1, -1, ms(t0));
    }

    /** B: as A, but the N+1 COPYs run concurrently on duplicated connections. */
    static Timing rawDirectParallel(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Map<String, Future<Long>> fs = new LinkedHashMap<>();
        fs.put("base", pool.submit(() -> copyOnDuplicate(baseOver(raw()), s3Path(prefix, "base"))));
        for (var sc : SIDECARS.entrySet()) {
            fs.put(sc.getKey(), pool.submit(() ->
                    copyOnDuplicate(sidecarOver(baseOver(raw()), sc.getValue()), s3Path(prefix, sc.getKey()))));
        }
        for (var f : fs.entrySet()) rc.put(f.getKey(), f.getValue().get());
        return new Timing(-1, -1, ms(t0));
    }

    /** C: the transformed base materialized once in a temp table; every output COPYs from it to S3. */
    static Timing tempTableDirect(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        try (var conn = (DuckDBConnection) db.duplicate()) {
            exec(conn, "CREATE TEMP TABLE __base_t AS " + baseOver(raw()));
            rc.put("base", copy(conn, "SELECT * FROM __base_t", s3Path(prefix, "base")));
            for (var sc : SIDECARS.entrySet()) {
                rc.put(sc.getKey(), copy(conn, sidecarOver("SELECT * FROM __base_t", sc.getValue()),
                        s3Path(prefix, sc.getKey())));
            }
            exec(conn, "DROP TABLE __base_t");
        }
        return new Timing(-1, -1, ms(t0));
    }

    /** D: base written to S3 first; each sidecar reads that Parquet back from S3. */
    static Timing s3Readback(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        String base = s3Path(prefix, "base");
        rc.put("base", copy(db, baseOver(raw()), base));
        for (var sc : SIDECARS.entrySet()) {
            rc.put(sc.getKey(), copy(db, sidecarOver("SELECT * FROM read_parquet('" + base + "')", sc.getValue()),
                    s3Path(prefix, sc.getKey())));
        }
        return new Timing(-1, -1, ms(t0));
    }

    /** E: every output derived from the raw Arrow files into local Parquet; then all uploaded in parallel. */
    static Timing rawLocalThenUpload(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Path dir = Files.createDirectories(work.resolve(prefix));
        Map<String, Path> out = new LinkedHashMap<>();
        out.put("base", dir.resolve("base.parquet"));
        rc.put("base", copy(db, baseOver(raw()), out.get("base").toString()));
        for (var sc : SIDECARS.entrySet()) {
            out.put(sc.getKey(), dir.resolve(sc.getKey() + ".parquet"));
            rc.put(sc.getKey(), copy(db, sidecarOver(baseOver(raw()), sc.getValue()), out.get(sc.getKey()).toString()));
        }
        long derive = ms(t0);
        long t1 = System.nanoTime();
        uploadAll(prefix, out);
        long upload = ms(t1);
        deleteDir(dir);
        return new Timing(derive, upload, ms(t0));
    }

    /** F: base to local Parquet; sidecars derived from that local Parquet; then all uploaded in parallel. */
    static Timing localParquetThenUpload(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Path dir = Files.createDirectories(work.resolve(prefix));
        Map<String, Path> out = new LinkedHashMap<>();
        out.put("base", dir.resolve("base.parquet"));
        rc.put("base", copy(db, baseOver(raw()), out.get("base").toString()));
        String fromBase = "SELECT * FROM read_parquet('" + out.get("base") + "')";
        for (var sc : SIDECARS.entrySet()) {
            out.put(sc.getKey(), dir.resolve(sc.getKey() + ".parquet"));
            rc.put(sc.getKey(), copy(db, sidecarOver(fromBase, sc.getValue()), out.get(sc.getKey()).toString()));
        }
        long derive = ms(t0);
        long t1 = System.nanoTime();
        uploadAll(prefix, out);
        long upload = ms(t1);
        deleteDir(dir);
        return new Timing(derive, upload, ms(t0));
    }

    /** G: as F, but the base file starts uploading as soon as it is written, while the sidecars are derived. */
    static Timing localParquetPipelined(String prefix, Map<String, Long> rc) throws Exception {
        long t0 = System.nanoTime();
        Path dir = Files.createDirectories(work.resolve(prefix));
        Path base = dir.resolve("base.parquet");
        rc.put("base", copy(db, baseOver(raw()), base.toString()));
        List<CompletableFuture<?>> uploads = new ArrayList<>();
        uploads.add(upload(prefix, "base", base));
        String fromBase = "SELECT * FROM read_parquet('" + base + "')";
        for (var sc : SIDECARS.entrySet()) {
            Path p = dir.resolve(sc.getKey() + ".parquet");
            rc.put(sc.getKey(), copy(db, sidecarOver(fromBase, sc.getValue()), p.toString()));
            uploads.add(upload(prefix, sc.getKey(), p));
        }
        long derive = ms(t0);
        long t1 = System.nanoTime();
        CompletableFuture.allOf(uploads.toArray(CompletableFuture[]::new)).join();
        long waitedForUpload = ms(t1);
        deleteDir(dir);
        return new Timing(derive, waitedForUpload, ms(t0));
    }

    // ---- helpers ---------------------------------------------------------------------------

    static String raw() {
        return "SELECT * FROM read_arrow([" + inputs.stream().map(p -> "'" + p + "'").collect(Collectors.joining(",")) + "])";
    }

    static String baseOver(String source) {
        return "WITH __this AS (" + source + ") " + BASE_TRANSFORMATION;
    }

    static String sidecarOver(String baseRows, String sidecar) {
        return "WITH __base AS (" + baseRows + ") " + sidecar;
    }

    static String s3Path(String prefix, String name) {
        return "s3://" + BUCKET + "/" + prefix + "/" + name + ".parquet";
    }

    static long copy(DuckDBConnection conn, String query, String target) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("COPY (" + query + ") TO '" + target + "' (FORMAT parquet)");
            return st.getUpdateCount();
        }
    }

    static long copyOnDuplicate(String query, String target) throws Exception {
        try (var conn = (DuckDBConnection) db.duplicate()) {
            return copy(conn, query, target);
        }
    }

    static CompletableFuture<?> upload(String prefix, String name, Path file) {
        return tm.uploadFile(UploadFileRequest.builder()
                .putObjectRequest(b -> b.bucket(BUCKET).key(prefix + "/" + name + ".parquet"))
                .source(file).build()).completionFuture();
    }

    static void uploadAll(String prefix, Map<String, Path> files) {
        CompletableFuture.allOf(files.entrySet().stream()
                .map(e -> upload(prefix, e.getKey(), e.getValue()))
                .toArray(CompletableFuture[]::new)).join();
    }

    static void verifyOnS3(String name, String prefix, Map<String, Long> rc) throws Exception {
        for (String out : rc.keySet()) {
            long n = Long.parseLong(scalar(db, "SELECT count(*) FROM read_parquet('" + s3Path(prefix, out) + "')"));
            if (n != rc.get(out)) throw new IllegalStateException(name + ": " + out + " has " + n + " rows on S3, wrote " + rc.get(out));
        }
    }

    static void cleanup(String prefix) {
        var keys = s3.listObjectsV2(b -> b.bucket(BUCKET).prefix(prefix + "/")).join().contents().stream()
                .map(o -> ObjectIdentifier.builder().key(o.key()).build()).toList();
        if (!keys.isEmpty()) {
            s3.deleteObjects(b -> b.bucket(BUCKET).delete(Delete.builder().objects(keys).build())).join();
        }
    }

    static List<Path> generateInput(int rows, int files) throws Exception {
        List<Path> out = new ArrayList<>();
        int per = rows / files;
        for (int i = 0; i < files; i++) {
            Path p = work.resolve("in_" + i + ".arrow");
            long off = (long) i * per;
            String gen = """
                    SELECT TIMESTAMP '2026-10-31 00:00:00' + to_seconds((r %% 86400)::BIGINT) AS ts,
                           'svc-' || (hash(r) %% 50) AS service,
                           CASE WHEN hash(r + 1) %% 20 = 0 THEN 'ERROR' WHEN hash(r + 1) %% 20 < 3 THEN 'WARN'
                                ELSE 'INFO' END AS severity,
                           md5(r::VARCHAR) AS trace_id,
                           'request handled for ' || md5((r + 7)::VARCHAR) || ' ' || md5((r + 11)::VARCHAR) AS body,
                           ['http.method', 'http.route', 'k' || (hash(r + 2) %% 200)] AS attr_keys,
                           [200, 200, 200, 404, 500][1 + (hash(r + 3) %% 5)::INT] AS http_status,
                           (hash(r + 4) %% 100000) / 100.0 AS duration_ms,
                           't' || (hash(r + 5) %% 10) AS tenant
                    FROM (SELECT range + %d AS r FROM range(%d))
                    """.formatted(off, per);
            exec(db, "COPY (" + gen + ") TO '" + p + "' (FORMAT arrow)");
            out.add(p);
        }
        return out;
    }

    static void exec(DuckDBConnection conn, String sql) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    static String scalar(DuckDBConnection conn, String sql) throws Exception {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    static long ms(long t0) {
        return (System.nanoTime() - t0) / 1_000_000;
    }

    static long median(java.util.stream.LongStream s) {
        long[] a = s.sorted().toArray();
        return a.length == 0 ? 0 : a[a.length / 2];
    }

    static void deleteDir(Path dir) throws Exception {
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
