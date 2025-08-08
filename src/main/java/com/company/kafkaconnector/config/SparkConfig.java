package com.company.kafkaconnector.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${aws.s3.endpoint:}")
    private String s3EndpointUrl;

    @Value("${aws.s3.access-key-id}")
    private String s3AccessKeyId;

    @Value("${aws.s3.secret-access-key}")
    private String s3SecretAccessKey;

    @Bean
    public SparkSession sparkSession() {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("Kafka S3 Delta Lake Connector")
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.access.key", s3AccessKeyId)
                .config("spark.hadoop.fs.s3a.secret.key", s3SecretAccessKey);

        if (s3EndpointUrl != null && !s3EndpointUrl.isEmpty()) {
            builder.config("spark.hadoop.fs.s3a.endpoint", s3EndpointUrl);
        }

        return builder.getOrCreate();
    }
}
