package org.hormigas.ws.ports.storage;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.storage.PresignedUrl;

/**
 * Driven port for object storage (MinIO/S3) used by the two-phase presigned-upload flow (ADR-010).
 * Binaries never transit the service: the client PUTs/GETs MinIO directly via short-lived presigned
 * URLs; the service only mints URLs and verifies/deletes objects by key. {@link PresignedUrl} is a
 * domain value object.
 */
public interface ObjectStorage {

    /** Presigned {@code PUT} URL the client uploads the bytes to, valid for {@code ttlSeconds}. */
    Uni<PresignedUrl> presignPut(String objectKey, String contentType, int ttlSeconds);

    /** Presigned {@code GET} URL for downloading the object, valid for {@code ttlSeconds}. */
    Uni<PresignedUrl> presignGet(String objectKey, int ttlSeconds);

    /** True iff the object actually exists (used at confirm to reject a never-uploaded key). */
    Uni<Boolean> exists(String objectKey);

    /** Actual stored size in bytes (confirm-time size enforcement against a lying client), or
     *  {@code -1} if the object is absent/unreadable. */
    Uni<Long> size(String objectKey);

    /** Best-effort delete (orphan cleanup); never fails the caller if the object is already gone. */
    Uni<Void> delete(String objectKey);
}
