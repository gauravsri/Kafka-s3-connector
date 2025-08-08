package com.company.kafkaconnector.config;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.defaults.engine.DefaultEngine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeltaKernelConfig {

    @Value("${aws.s3.endpoint:}")
    private String s3EndpointUrl;

    @Value("${aws.s3.access-key-id}")
    private String s3AccessKeyId;

    @Value("${aws.s3.secret-access-key}")
    private String s3SecretAccessKey;

    @Bean
    public Engine deltaKernelEngine() {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        if (s3EndpointUrl != null && !s3EndpointUrl.isEmpty()) {
            hadoopConf.set("fs.s3a.endpoint", s3EndpointUrl);
        }
        hadoopConf.set("fs.s3a.access.key", s3AccessKeyId);
        hadoopConf.set("fs.s3a.secret.key", s3SecretAccessKey);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        return DefaultEngine.create(hadoopConf);
    }
}
