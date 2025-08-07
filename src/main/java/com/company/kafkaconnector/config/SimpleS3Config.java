package com.company.kafkaconnector.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

@Configuration
public class SimpleS3Config {

    @Value("${aws.s3.endpoint:}")
    private String endpoint;

    @Value("${aws.s3.region}")
    private String region;

    @Value("${aws.s3.access-key-id}")
    private String accessKeyId;

    @Value("${aws.s3.secret-access-key}")
    private String secretAccessKey;

    @Value("${aws.s3.path-style-access:false}")
    private boolean pathStyleAccess;

    @Bean
    public S3Client s3Client() {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(region));

        // Configure credentials
        if (!accessKeyId.isEmpty() && !secretAccessKey.isEmpty()) {
            AwsCredentialsProvider credentials = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)
            );
            builder.credentialsProvider(credentials);
        }

        // Configure endpoint for MinIO or custom S3 endpoint
        if (!endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }

        // Enable path-style access for MinIO
        if (pathStyleAccess) {
            builder.forcePathStyle(true);
        }

        return builder.build();
    }
}