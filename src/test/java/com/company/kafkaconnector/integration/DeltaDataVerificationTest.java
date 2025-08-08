package com.company.kafkaconnector.integration;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "RUN_DATA_VERIFICATION", matches = "true")
public class DeltaDataVerificationTest {

    private static final String BUCKET_NAME = "test-data-lake";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";

    private static Engine engine;

    @BeforeAll
    static void setUp() {
        log.info("Setting up Delta Lake data verification test");
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT);
        hadoopConf.set("fs.s3a.access.key", ACCESS_KEY);
        hadoopConf.set("fs.s3a.secret.key", SECRET_KEY);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        engine = DefaultEngine.create(hadoopConf);
        log.info("Delta Kernel engine created for Delta Lake verification");
    }

    @AfterAll
    static void tearDown() {
    }

    @Test
    @Order(1)
    @DisplayName("Test 1: Verify user_events Delta table existence and content")
    void testUserEventsDeltaTable() {
        log.info("Verifying user_events Delta table");
        String tablePath = String.format("s3a://%s/events/user-events", BUCKET_NAME);

        Table table = Table.forPath(engine, tablePath);
        Snapshot snapshot = table.getLatestSnapshot(engine);
        StructType schema = snapshot.getSchema();
        Scan scan = snapshot.getScanBuilder().build();
        CloseableIterator<FilteredColumnarBatch> data = scan.getScanFiles(engine);

        long rowCount = 0;
        while(data.hasNext()){
            rowCount += data.next().getData().getSize();
        }

        assertThat(rowCount).isGreaterThan(0);
        assertThat(schema.fieldNames()).contains("user_id", "event_type", "timestamp", "properties");

        log.info("✅ user_events Delta table verified with {} rows", rowCount);
    }

    @Test
    @Order(2)
    @DisplayName("Test 2: Verify order_events Delta table existence and content")
    void testOrderEventsDeltaTable() {
        log.info("Verifying order_events Delta table");
        String tablePath = String.format("s3a://%s/orders/order-events", BUCKET_NAME);

        Table table = Table.forPath(engine, tablePath);
        Snapshot snapshot = table.getLatestSnapshot(engine);
        StructType schema = snapshot.getSchema();
        Scan scan = snapshot.getScanBuilder().build();
        CloseableIterator<FilteredColumnarBatch> data = scan.getScanFiles(engine);

        long rowCount = 0;
        while(data.hasNext()){
            rowCount += data.next().getData().getSize();
        }

        assertThat(rowCount).isGreaterThan(0);
        assertThat(schema.fieldNames()).contains("order_id", "product_id", "quantity", "price", "timestamp");

        log.info("✅ order_events Delta table verified with {} rows", rowCount);
    }
}
