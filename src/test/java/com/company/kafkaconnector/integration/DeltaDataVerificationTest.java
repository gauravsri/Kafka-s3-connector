package com.company.kafkaconnector.integration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "RUN_DATA_VERIFICATION", matches = "true")
public class DeltaDataVerificationTest {

    private static final String BUCKET_NAME = "test-data-lake";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        log.info("Setting up Delta Lake data verification test");
        spark = SparkSession.builder()
                .appName("DeltaDataVerificationTest")
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
        log.info("Spark session created for Delta Lake verification");
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test 1: Verify user_events Delta table existence and content")
    void testUserEventsDeltaTable() {
        log.info("Verifying user_events Delta table");
        String tablePath = String.format("s3a://%s/events/user-events", BUCKET_NAME);

        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        df.show(5, false);

        assertThat(df.count()).isGreaterThan(0);
        assertThat(df.columns()).contains("user_id", "event_type", "timestamp", "properties");

        log.info("✅ user_events Delta table verified with {} rows", df.count());
    }

    @Test
    @Order(2)
    @DisplayName("Test 2: Verify order_events Delta table existence and content")
    void testOrderEventsDeltaTable() {
        log.info("Verifying order_events Delta table");
        String tablePath = String.format("s3a://%s/orders/order-events", BUCKET_NAME);

        Dataset<Row> df = spark.read().format("delta").load(tablePath);
        df.show(5, false);

        assertThat(df.count()).isGreaterThan(0);
        assertThat(df.columns()).contains("order_id", "product_id", "quantity", "price", "timestamp");

        log.info("✅ order_events Delta table verified with {} rows", df.count());
    }
}
