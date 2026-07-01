package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;
import java.util.Optional;

/**
 * Attachment upload config (concept §10, ADR-010). Two-phase presigned upload: size is enforced both at
 * request (declared) and at confirm (actual, via MinIO stat); content-type is whitelisted.
 */
@ConfigMapping(prefix = "processing.attachments")
public interface AttachmentsConfig {

    /** Hard cap on a single upload. ↑ permits larger files (more storage + bigger GET transfers); ↓ rejects more early. */
    @WithDefault("26214400")
    long maxSizeBytes();

    /** Allowed content-types (exact or "type/*" wildcard). Absent/empty = allow all; set to lock down (→415). */
    Optional<List<String>> allowedContentTypes();

    /** Grace before the reaper reclaims a never-confirmed PENDING upload. Must exceed {@code minio.upload-ttl-seconds}. */
    @WithDefault("3600")
    long orphanAgeSeconds();

    /** Orphan-reclaim page size per cleanup tick. ↑ drains faster, heavier DB/MinIO load per tick; ↓ gentler. */
    @WithDefault("200")
    int cleanupBatch();
}
