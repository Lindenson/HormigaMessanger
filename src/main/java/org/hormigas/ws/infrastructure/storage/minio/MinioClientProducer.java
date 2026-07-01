package org.hormigas.ws.infrastructure.storage.minio;

import io.minio.MinioClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.hormigas.ws.config.MinioConfig;

/**
 * Produces the MinIO client from config. The endpoint is the URL the service signs against, so it
 * MUST also be the host clients can reach (the platform's {@code MINIO_PUBLIC_URL} split-horizon
 * concern, ADR-010); for the e2e/dev env service and clients share {@code localhost:9000}.
 * No connection is opened here — the client is lazy, so startup never depends on MinIO being up.
 */
@ApplicationScoped
public class MinioClientProducer {

    @Inject
    MinioConfig config;

    @Produces
    @ApplicationScoped
    public MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(config.endpoint())
                .credentials(config.accessKey(), config.secretKey())
                .build();
    }
}
