package kr.co.idgenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import kr.co.idgenerator.strategy.IdGenerator;
import kr.co.idgenerator.strategy.TsidGenerator;
import kr.co.idgenerator.strategy.UuidGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IdGeneratorBenchmarkTest {
    private static final int THREAD_POOL_SIZE = 10;
    private static final int TOTAL_BENCHMARK_SIZE = 10_000_000; // 총 1000만건 테스트
    private static final Map<String, List<BenchmarkResult>> results = new HashMap<>();
    private static Connection conn;

    @AfterAll
    static void teardown() throws Exception {
        // 테이블은 삭제하지 않고 유지 (통계 확인용)
        conn.close();
        ResultWriter.exportToExcel(results);
    }

    private static void createTables() throws SQLException {
        final Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE IF NOT EXISTS `UUID_TABLE` (id BINARY(16) PRIMARY KEY) ENGINE=InnoDB");
        stmt.execute("CREATE TABLE IF NOT EXISTS `TSID_TABLE` (id VARCHAR(13) PRIMARY KEY) ENGINE=InnoDB");
    }

    private static void dropTables() throws SQLException {
        final Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS `UUID_TABLE`");
        stmt.execute("DROP TABLE IF EXISTS `TSID_TABLE`");
    }

    @BeforeAll
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:13306/benchmark?createDatabaseIfNotExist=true&rewriteBatchedStatements=true",
                "root", "password");
        dropTables(); // 시작 전에 기존 테이블 삭제
        createTables();
    }

    @ParameterizedTest(name = "UUID")
    @ValueSource(ints = {1_000_000})
    public void evaluateUUID(final int sampleSize) throws Exception {
        final IdGenerator<String> generator = new UuidGenerator();
        runBenchmark(generator, "UUID", sampleSize);
    }

    @ParameterizedTest(name = "TSID")
    @ValueSource(ints = {1_000_000})
    public void evaluateTSID(final int sampleSize) throws Exception {
        final IdGenerator<String> generator = new TsidGenerator();
        runBenchmark(generator, "TSID", sampleSize);
    }

    private <E extends Comparable<? super E>> void runBenchmark(final IdGenerator<E> generator,
                                                                final String generatorName, final int batchSize)
            throws Exception {
        final BenchmarkResult result = new BenchmarkResult(generatorName, TOTAL_BENCHMARK_SIZE);

        long totalGenerationTime = 0;
        double totalCollisionRate = 0;
        long totalDbInsertTime = 0;

        // 테이블 초기화
        truncateTable(generatorName);

        // 총 1000만건을 batchSize씩 나눠서 INSERT (누적)
        final int iterations = TOTAL_BENCHMARK_SIZE / batchSize;
        for (int i = 0; i < iterations; i++) {
            // ID 생성 시간 측정
            final long start = System.nanoTime();
            final List<E> ids = generateIds(generator, batchSize);
            final long genTime = (System.nanoTime() - start) / 1_000_000;
            totalGenerationTime += genTime;

            totalCollisionRate += testCollisionRate(ids, batchSize);
            final long insertTime = testDbInsertPerformance(ids, generatorName, false); // 누적 INSERT
            totalDbInsertTime += insertTime;

            // 진행 상황 로그 출력
            System.out.printf("[%s] Progress: %d/%d rows, Generation=%dms, Insert=%dms\n",
                    generatorName, (i + 1) * batchSize, TOTAL_BENCHMARK_SIZE, genTime, insertTime);
        }

        result.setGenerationTime(totalGenerationTime / iterations);
        result.setCollisionRate(totalCollisionRate / iterations);
        result.setDbInsertTime(totalDbInsertTime / iterations);

        final E sampleId = generator.execute();
        final String exampleId = sampleId.toString();
        result.setExampleId(exampleId);

        // DB 실제 저장 크기 (UUID: BINARY(16), TSID: VARCHAR(13))
        final int dbStorageSize = generatorName.equals("UUID") ? 16 : 13;
        result.setByteSize(dbStorageSize);

        results.computeIfAbsent(generatorName, k -> new ArrayList<>()).add(result);
    }

    // ID 생성 (멀티스레드 동시 생성, 태스크별 고정 구간 없음)
    private <E> List<E> generateIds(final IdGenerator<E> generator, final int sampleSize) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        final CountDownLatch latch = new CountDownLatch(THREAD_POOL_SIZE);
        final CountDownLatch startSignal = new CountDownLatch(1);
        final AtomicInteger next = new AtomicInteger(0);
        final Object[] buffer = new Object[sampleSize];

        for (int t = 0; t < THREAD_POOL_SIZE; t++) {
            executor.submit(() -> {
                try {
                    // 모든 스레드가 준비될 때까지 대기
                    startSignal.await();
                    while (true) {
                        final int idx = next.getAndIncrement();
                        if (idx >= sampleSize) {
                            break;
                        }
                        buffer[idx] = generator.execute();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 모든 스레드 시작
        startSignal.countDown();

        try {
            latch.await();
        } finally {
            executor.shutdown();
        }

        final List<E> ids = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            @SuppressWarnings("unchecked") final E e = (E) buffer[i];
            ids.add(e);
        }
        return ids;
    }

    // ID 충돌율 테스트
    private <E> double testCollisionRate(final List<E> ids, final int sampleSize) {
        final Set<E> uniqueIds = new HashSet<>(ids);
        return 1 - ((double) uniqueIds.size() / sampleSize);
    }

    // 테이블 초기화
    private void truncateTable(final String generatorName) throws SQLException {
        try (final Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + generatorName + "_TABLE");
        }
    }

    // DB 삽입 성능 테스트
    private <E> long testDbInsertPerformance(final List<E> ids, final String generatorName, final boolean truncate)
            throws SQLException {
        if (truncate) {
            truncateTable(generatorName);
        }

        // 순수 INSERT 시간만 측정 (Batch 사용)
        final long start = System.nanoTime();

        // UUID는 BINARY(16)로 저장하므로 UUID_TO_BIN() 사용
        final String sql = generatorName.equals("UUID")
                ? "INSERT INTO " + generatorName + "_TABLE (id) VALUES (UUID_TO_BIN(?))"
                : "INSERT INTO " + generatorName + "_TABLE (id) VALUES (?)";

        try (final PreparedStatement pstmt = conn.prepareStatement(sql)) {
            final int batchSize = 1000;
            for (int i = 0; i < ids.size(); i++) {
                final E id = ids.get(i);
                if (id instanceof String) {
                    pstmt.setString(1, (String) id);
                } else if (id instanceof Long) {
                    pstmt.setLong(1, (Long) id);
                }
                pstmt.addBatch();

                // 1000개씩 배치 실행
                if ((i + 1) % batchSize == 0) {
                    pstmt.executeBatch();
                }
            }
            // 남은 것 실행
            pstmt.executeBatch();
        }

        return (System.nanoTime() - start) / 1_000_000;
    }

}
