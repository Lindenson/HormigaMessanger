package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * MinIO / object-storage config (ADR-010). The endpoint is signed into presigned URLs, so it MUST be a
 * host clients can also reach (split-horizon); the client is lazy so startup never depends on MinIO.
 */
@ConfigMapping(prefix = "minio")
public interface MinioConfig {

    /** MinIO/S3 endpoint — baked into presigned URLs, so it must be browser-reachable per environment. */
    @WithDefault("http://localhost:9000")
    String endpoint();

    @WithDefault("hormiga")
    String accessKey();

    @WithDefault("hormiga123")
    String secretKey();

    @WithDefault("messenger-attachments")
    String bucket();

    /** Presigned PUT lifetime. ↑ gives slow clients more time; ↓ tightens the upload window. Keep ≥ typical upload duration. */
    @WithDefault("600")
    int uploadTtlSeconds();

    /** Presigned GET lifetime. ↑ lets a URL be reused/shared longer; ↓ limits leaked-URL exposure. */
    @WithDefault("300")
    int downloadTtlSeconds();
}
