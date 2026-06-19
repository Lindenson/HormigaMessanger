package org.hormigas.ws.ports.storage;

import io.smallrye.mutiny.Uni;

import java.time.Instant;

/**
 * Driven port for object storage (MinIO/S3) used by the two-phase presigned-upload flow (ADR-010).
 * Binaries never transit the service: the client PUTs/GETs MinIO directly via short-lived presigned
 * URLs; the service only mints URLs and verifies/deletes objects by key.
 */
public interface ObjectStorage {

    /** A short-lived presigned URL plus the method the client must use and its expiry. */
    record PresignedUrl(String url, String method, Instant expiresAt) {}

    /** Presigned {@code PUT} URL the client uploads the bytes to, valid for {@code ttlSeconds}. */
    Uni<PresignedUrl> presignPut(String objectKey, String contentType, int ttlSeconds);

    /** Presigned {@code GET} URL for downloading the object, valid for {@code ttlSeconds}. */
    Uni<PresignedUrl> presignGet(String objectKey, int ttlSeconds);

    /** True iff the object actually exists (used at confirm to reject a never-uploaded key). */
    Uni<Boolean> exists(String objectKey);

    /** Best-effort delete (orphan cleanup); never fails the caller if the object is already gone. */
    Uni<Void> delete(String objectKey);
}
