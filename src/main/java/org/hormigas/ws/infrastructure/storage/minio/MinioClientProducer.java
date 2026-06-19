package org.hormigas.ws.infrastructure.storage.minio;

import io.minio.MinioClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Produces the MinIO client from config. The endpoint is the URL the service signs against, so it
 * MUST also be the host clients can reach (the platform's {@code MINIO_PUBLIC_URL} split-horizon
 * concern, ADR-010); for the e2e/dev env service and clients share {@code localhost:9000}.
 * No connection is opened here — the client is lazy, so startup never depends on MinIO being up.
 */
@ApplicationScoped
public class MinioClientProducer {

    @ConfigProperty(name = "minio.endpoint", defaultValue = "http://localhost:9000")
    String endpoint;

    @ConfigProperty(name = "minio.access-key", defaultValue = "hormiga")
    String accessKey;

    @ConfigProperty(name = "minio.secret-key", defaultValue = "hormiga123")
    String secretKey;

    @Produces
    @ApplicationScoped
    public MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
    }
}
